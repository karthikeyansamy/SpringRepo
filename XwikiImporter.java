package com.example.migrator;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.hc.client5.http.classic.methods.HttpGet;
import org.apache.hc.client5.http.classic.methods.HttpPost;
import org.apache.hc.client5.http.entity.EntityBuilder;
import org.apache.hc.client5.http.entity.mime.MultipartEntityBuilder;
import org.apache.hc.client5.http.impl.classic.CloseableHttpClient;
import org.apache.hc.client5.http.impl.classic.HttpClients;
import org.apache.hc.core5.http.ContentType;
import org.apache.hc.core5.http.io.entity.StringEntity;
import org.apache.hc.core5.http.io.entity.EntityUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;
import java.util.Base64;
import java.util.concurrent.TimeUnit;

/**
 * XWikiImporter - uses Filter Streams Converter + Confluence XML filter.
 * - uploadZip(...) optional : uploads file as a resource and returns the returned reference
 * - importFromFileReference(...) : starts a filter job using the given reference (file:/... or resource reference)
 * - waitForJobCompletion(...) : polls job status
 */
public class XWikiImporter {
    private static final Logger logger = LoggerFactory.getLogger(XWikiImporter.class);
    private final String baseUrl;    // e.g. http://xwiki.example.com
    private final String user;
    private final String pass;
    private final ObjectMapper mapper = new ObjectMapper();

    public XWikiImporter(String baseUrl, String user, String pass) {
        this.baseUrl = baseUrl.replaceAll("/+$", "");
        this.user = user;
        this.pass = pass;
    }

    private String basicAuth() {
        String raw = user + ":" + pass;
        return "Basic " + Base64.getEncoder().encodeToString(raw.getBytes());
    }

    /**
     * Option A: Upload the zip to XWiki (REST resource) and return the 'reference' string.
     * POST /rest/wikis/xwiki/resources
     */
    public String uploadZip(File zipFile) throws Exception {
        String url = baseUrl + "/rest/wikis/xwiki/resources";
        logger.info("Uploading zip to XWiki resources: {}", zipFile.getName());

        try (CloseableHttpClient http = HttpClients.createDefault()) {
            HttpPost post = new HttpPost(url);
            post.addHeader("Authorization", basicAuth());

            MultipartEntityBuilder mb = MultipartEntityBuilder.create();
            mb.addBinaryBody("file", zipFile, ContentType.DEFAULT_BINARY, zipFile.getName());

            post.setEntity(mb.build());

            try (var resp = http.execute(post)) {
                int sc = resp.getCode();
                String body = resp.getEntity() != null ? EntityUtils.toString(resp.getEntity()) : null;
                if (sc < 200 || sc >= 300) {
                    logger.error("Upload failed HTTP {} -> {}", sc, body);
                    throw new RuntimeException("Upload failed HTTP " + sc + " -> " + body);
                }
                JsonNode json = mapper.readTree(body);
                String reference = json.path("reference").asText(null);
                logger.info("Upload succeeded, reference={}", reference);
                return reference;
            }
        }
    }

    /**
     * Option B: Direct use of a file: reference on the XWiki server.
     * Example reference: "file:/path/on/xwiki/server/confluence-export.zip"
     *
     * The docs recommend using file: when the application server can access the path. :contentReference[oaicite:5]{index=5}
     */
    public void importFromFileReference(String fileReference) throws Exception {
        logger.info("Starting filter job using reference: {}", fileReference);

        // Build the job request per Filter Streams Converter expected shape
        ObjectNode job = mapper.createObjectNode();
        job.put("jobType", "filter");

        // jobId array: example ["confluence", "import"]
        var jobIdArr = job.putArray("jobId");
        jobIdArr.add("confluence");
        jobIdArr.add("import");

        ObjectNode request = job.putObject("request");

        // input: { type: "file", reference: "file:/..." }  OR reference obtained after upload
        ObjectNode input = request.putObject("input");
        input.put("type", "file");
        input.put("reference", fileReference);

        // output: wiki instance importer
        ObjectNode output = request.putObject("output");
        output.put("type", "wiki");
        ObjectNode params = output.putObject("parameters");
        params.put("targetWiki", "xwiki"); // change if your wiki id differs

        // filter and flags
        request.put("filter", "confluence+xml");
        request.put("verbose", true);
        request.put("interactive", false);

        String url = baseUrl + "/rest/jobs";
        try (CloseableHttpClient http = HttpClients.createDefault()) {
            HttpPost post = new HttpPost(url);
            post.addHeader("Authorization", basicAuth());
            post.addHeader("Content-Type", "application/json");
            StringEntity ent = new StringEntity(mapper.writeValueAsString(job), ContentType.APPLICATION_JSON);
            post.setEntity(ent);

            try (var resp = http.execute(post)) {
                int sc = resp.getCode();
                String body = resp.getEntity() != null ? EntityUtils.toString(resp.getEntity()) : null;
                if (sc < 200 || sc >= 300) {
                    logger.error("Failed to start job HTTP {} -> {}", sc, body);
                    throw new RuntimeException("Failed to start job HTTP " + sc + " -> " + body);
                }
                logger.info("Import job triggered successfully. Response: {}", body);
            }
        }
    }

    /**
     * Poll the converter job status. The Confluence XML docs show the jobs endpoint to poll (e.g. /rest/jobs/confluence/import). :contentReference[oaicite:6]{index=6}
     * This waits until status == finished or failed (timeout after given duration).
     */
    public void waitForJobCompletion(Duration timeout) throws Exception {
        String url = baseUrl + "/rest/jobs/confluence/import";
        Instant deadline = Instant.now().plus(timeout);

        logger.info("Polling import job status at {}", url);

        try (CloseableHttpClient http = HttpClients.createDefault()) {
            while (Instant.now().isBefore(deadline)) {
                HttpGet get = new HttpGet(url);
                get.addHeader("Authorization", basicAuth());
                get.addHeader("Accept", "application/json");

                try (var resp = http.execute(get)) {
                    int sc = resp.getCode();
                    String body = resp.getEntity() != null ? EntityUtils.toString(resp.getEntity()) : "{}";
                    if (sc >= 200 && sc < 300) {
                        JsonNode json = mapper.readTree(body);
                        String status = json.path("status").asText(null);
                        int progress = json.path("progress").asInt(-1);
                        logger.info("Import status: {} ({}%)", status, progress);
                        if ("finished".equalsIgnoreCase(status)) {
                            logger.info("Import finished successfully");
                            return;
                        }
                        if ("failed".equalsIgnoreCase(status)) {
                            logger.error("Import failed: {}", body);
                            throw new RuntimeException("Import failed: " + body);
                        }
                    } else {
                        logger.warn("Polling returned HTTP {} -> {}", sc, body);
                    }
                }

                TimeUnit.SECONDS.sleep(5);
            }
        }

        throw new RuntimeException("Timed out waiting for import job to finish");
    }

    // Convenience end-to-end helper: upload (optional) then import via fileRef
    public void importZipViaFilterStream(File zipFile, boolean uploadFirst, Duration timeout) throws Exception {
        String fileRef;
        if (uploadFirst) {
            fileRef = uploadZip(zipFile);
            if (fileRef == null) {
                throw new RuntimeException("Upload returned null reference");
            }
        } else {
            // Use a file: reference. IMPORTANT: this path must be readable by the XWiki app server.
            // Developer instruction: using uploaded file path from session as example:
            String serverPath = "file:/mnt/data/17636415221346302215000371163129.jpg";
            // Replace the above with your actual ZIP path on the XWiki server when running for real.
            fileRef = serverPath;
            logger.info("Using file reference (no upload): {}", fileRef);
        }

        importFromFileReference(fileRef);
        waitForJobCompletion(timeout);
    }
}
