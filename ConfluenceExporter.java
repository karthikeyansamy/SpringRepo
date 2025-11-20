package com.example.migrator;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.hc.client5.http.classic.methods.HttpGet;
import org.apache.hc.client5.http.classic.methods.HttpPost;
import org.apache.hc.client5.http.entity.EntityBuilder;
import org.apache.hc.client5.http.impl.classic.CloseableHttpClient;
import org.apache.hc.client5.http.impl.classic.HttpClients;
import org.apache.hc.core5.http.ContentType;
import org.apache.hc.core5.http.io.entity.EntityUtils;

import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.time.Duration;
import java.time.Instant;
import java.util.Base64;
import java.util.List;
import java.util.concurrent.TimeUnit;

public class ConfluenceExporter {
    private final String baseUrl;
    private final String user;
    private final String pass;
    private final ObjectMapper mapper = new ObjectMapper();

    public ConfluenceExporter(String baseUrl, String user, String pass) {
        this.baseUrl = baseUrl.replaceAll("/+$", "");
        this.user = user;
        this.pass = pass;
    }

    /**
     * Triggers a Confluence DC space backup using the backup-restore API, polls the job, downloads the ZIP.
     * Returns the downloaded ZIP Path or null if dryRun.
     */
    public Path exportSpaceBackup(String spaceKey, Path downloadDir, boolean dryRun) throws Exception {
        if (dryRun) {
            System.out.println("[DRY-RUN] Would POST /rest/api/backup-restore/backup/space for space: " + spaceKey);
            return null;
        }

        Files.createDirectories(downloadDir);

        String jobId = triggerSpaceBackupJob(spaceKey);
        System.out.println("Triggered backup jobId=" + jobId);

        JsonNode job = pollJobUntilFinished(jobId, Duration.ofMinutes(30), Duration.ofSeconds(5));
        System.out.println("Job finished: " + job.toString());

        // Download via the job download endpoint
        Path zip = downloadJobFile(jobId, downloadDir);
        System.out.println("Downloaded backup to: " + zip.toAbsolutePath());
        return zip;
    }

    private String triggerSpaceBackupJob(String spaceKey) throws Exception {
        String url = baseUrl + "/rest/api/backup-restore/backup/space";

        try (CloseableHttpClient http = HttpClients.createDefault()) {
            // Request JSON: { "spaceKeys": ["KEY"], "keepPermanently": false }
            JsonNode payload = mapper.createObjectNode()
                    .putArray("spaceKeys").add(spaceKey)
                    .put("keepPermanently", false);

            HttpPost post = new HttpPost(url);
            post.addHeader("Authorization", basicAuthHeader());
            post.addHeader("Accept", "application/json");
            post.setEntity(EntityBuilder.create()
                    .setText(mapper.writeValueAsString(payload))
                    .setContentType(ContentType.APPLICATION_JSON)
                    .build());

            try (var resp = http.execute(post)) {
                int sc = resp.getCode();
                String body = resp.getEntity() == null ? null : EntityUtils.toString(resp.getEntity());
                if (sc < 200 || sc >= 300) {
                    throw new RuntimeException("Space backup trigger failed: HTTP " + sc + " -> " + body);
                }
                JsonNode j = mapper.readTree(body);
                // response commonly contains "id" or "jobId"
                if (j.has("id")) return j.get("id").asText();
                if (j.has("jobId")) return j.get("jobId").asText();
                // fallback: if response contains 'job' object
                if (j.has("job") && j.get("job").has("id")) return j.get("job").get("id").asText();
                throw new RuntimeException("Cannot find job id in response: " + body);
            }
        }
    }

    private JsonNode pollJobUntilFinished(String jobId, Duration timeout, Duration pollInterval) throws Exception {
        String url = baseUrl + "/rest/api/backup-restore/jobs/" + jobId;
        Instant end = Instant.now().plus(timeout);

        try (CloseableHttpClient http = HttpClients.createDefault()) {
            while (Instant.now().isBefore(end)) {
                HttpGet get = new HttpGet(url);
                get.addHeader("Authorization", basicAuthHeader());
                get.addHeader("Accept", "application/json");
                try (var resp = http.execute(get)) {
                    int sc = resp.getCode();
                    String body = resp.getEntity() == null ? null : EntityUtils.toString(resp.getEntity());
                    if (sc >= 200 && sc < 300 && body != null) {
                        JsonNode job = mapper.readTree(body);
                        // job state field in docs: job.state or job.jobState; check possible fields
                        String state = null;
                        if (job.has("state")) state = job.get("state").asText();
                        else if (job.has("jobState")) state = job.get("jobState").asText();
                        else if (job.has("job") && job.get("job").has("state")) state = job.get("job").get("state").asText();

                        if (state != null) {
                            if (state.equalsIgnoreCase("FINISHED") || state.equalsIgnoreCase("SUCCESS") || state.equalsIgnoreCase("COMPLETED")) {
                                return job;
                            }
                            if (state.equalsIgnoreCase("FAILED") || state.equalsIgnoreCase("CANCELLED")) {
                                throw new RuntimeException("Backup job ended with state=" + state + " -> " + job.toString());
                            }
                        }
                        // Some builds return a jobs JSON with 'jobState' or 'jobStatus' or nested; also check for 'status' text
                        if (body.toLowerCase().contains("finished") || body.toLowerCase().contains("success")) {
                            return job;
                        }
                        System.out.println("Backup job " + jobId + " status: " + (state == null ? body : state) + " — waiting...");
                    } else {
                        System.out.println("Status poll returned HTTP " + sc + " — body: " + body);
                    }
                }
                TimeUnit.MILLISECONDS.sleep(pollInterval.toMillis());
            }
        }
        throw new RuntimeException("Timeout waiting for backup job " + jobId + " to finish");
    }

    private Path downloadJobFile(String jobId, Path downloadDir) throws Exception {
        String url = baseUrl + "/rest/api/backup-restore/jobs/" + jobId + "/download";
        try (CloseableHttpClient http = HttpClients.createDefault()) {
            HttpGet get = new HttpGet(url);
            get.addHeader("Authorization", basicAuthHeader());
            try (var resp = http.execute(get)) {
                int sc = resp.getCode();
                if (sc == 200) {
                    // derive filename from Content-Disposition if present
                    String cd = resp.getFirstHeader("Content-Disposition") != null ?
                            resp.getFirstHeader("Content-Disposition").getValue() : null;
                    String filename = null;
                    if (cd != null && cd.contains("filename=")) {
                        filename = cd.substring(cd.indexOf("filename=") + 9).replaceAll("\"", "");
                    }
                    if (filename == null) filename = "confluence-space-backup-" + jobId + ".zip";
                    Path out = downloadDir.resolve(filename);
                    try (InputStream is = resp.getEntity().getContent()) {
                        Files.copy(is, out, StandardCopyOption.REPLACE_EXISTING);
                    }
                    return out;
                } else {
                    String body = resp.getEntity() == null ? null : EntityUtils.toString(resp.getEntity());
                    throw new RuntimeException("Download failed: HTTP " + sc + " -> " + body);
                }
            }
        }
    }

    private String basicAuthHeader() {
        String s = user + ":" + pass;
        return "Basic " + Base64.getEncoder().encodeToString(s.getBytes());
    }
}
