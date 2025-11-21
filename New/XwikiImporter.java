package com.example.migrator;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.hc.client5.http.classic.methods.HttpGet;
import org.apache.hc.client5.http.classic.methods.HttpPost;
import org.apache.hc.client5.http.config.RequestConfig;
import org.apache.hc.client5.http.entity.mime.MultipartEntityBuilder;
import org.apache.hc.client5.http.entity.mime.ContentBody;
import org.apache.hc.client5.http.entity.mime.FileBody;
import org.apache.hc.client5.http.impl.classic.CloseableHttpClient;
import org.apache.hc.client5.http.impl.classic.HttpClients;
import org.apache.hc.core5.http.ContentType;
import org.apache.hc.core5.http.HttpEntity;
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
 * XWikiImporter — triggers Confluence XML import via the Confluence XML add-on (Filter Stream)
 *
 * Behavior:
 *  - POST /rest/jobs/confluencexmlimport/{jobId} (multipart/form-data)
 *  - Poll   GET /rest/jobs/confluencexmlimport/{jobId} until finished/failed
 *
 * This implementation is defensive — it includes multiple parameter name variants that the extension
 * codebase / different versions sometimes expect (e.g. inputPackage vs file; preserveHistory vs importHistory).
 *
 * Sources:
 *  - Confluence XML extension docs (parameters & usage). :contentReference[oaicite:2]{index=2}
 *  - xwiki-contrib/confluence (confluence-xml module) implementation (parameter names & job wiring). :contentReference[oaicite:3]{index=3}
 */
public class XWikiImporter {
    private static final Logger logger = LoggerFactory.getLogger(XWikiImporter.class);
    private final String baseUrl;
    private final String user;
    private final String pass;
    private final ObjectMapper mapper = new ObjectMapper();

    public XWikiImporter(String baseUrl, String user, String pass) {
        this.baseUrl = baseUrl.replaceAll("/+$", "");
        this.user = user;
        this.pass = pass;
    }

    private String basicAuthHeader() {
        String raw = user + ":" + pass;
        return "Basic " + Base64.getEncoder().encodeToString(raw.getBytes());
    }

    /**
     * Trigger an import job using the Confluence XML add-on (Filter Stream).
     *
     * This method builds a multipart/form-data request containing both the "recommended" parameters
     * and alternative names commonly found in the extension implementation. That makes it tolerant
     * to minor differences in installed versions.
     *
     * @param exportZipPath   local zip file to upload (set null if using server-side fileRef)
     * @param serverFileRef   optional server-side file reference (file:/... or resource reference) — used when exportZipPath==null
     * @param jobId           unique job id (used in URL); choose e.g. spaceName + timestamp
     * @param wikidocument    target wiki document (e.g. "MySpace.WebHome") — where import root will be created
     * @param defaultSpace    default space name (fallback)
     * @param strategy        import strategy: "replace" | "append" (other values may be accepted)
     * @param importHistory   import history (true/false)
     * @param importComments  import comments (true/false)
     * @param importAttachments import attachments (true/false)
     * @param importProfiles  import user profiles (true/false)
     * @param importUnresolvedReferences import unresolved refs (true/false)
     * @param filterLogLevel  log level for filter (e.g. "INFO","DEBUG")
     * @param strictMode      strict mode for filter (true/false)
     * @param ignoreXmlHeader ignore xml header checks (true/false)
     * @param cleanEmptySpaces remove empty spaces after import (true/false)
     * @throws Exception on error
     */
    public void triggerImport(
            Path exportZipPath,
            String serverFileRef,
            String jobId,
            String wikidocument,
            String defaultSpace,
            String strategy,
            boolean importHistory,
            boolean importComments,
            boolean importAttachments,
            boolean importProfiles,
            boolean importUnresolvedReferences,
            String filterLogLevel,
            boolean strictMode,
            boolean ignoreXmlHeader,
            boolean cleanEmptySpaces
    ) throws Exception {

        String url = baseUrl + "/rest/jobs/confluencexmlimport/" + jobId;
        logger.info("Triggering Confluence XML import job: {} (jobId={})", url, jobId);

        HttpPost post = new HttpPost(url);
        post.addHeader("Authorization", basicAuthHeader());
        post.addHeader("Accept", "application/json");

        MultipartEntityBuilder mb = MultipartEntityBuilder.create();

        // Preferred parameter: upload file
        if (exportZipPath != null) {
            File f = exportZipPath.toFile();
            logger.info("Attaching ZIP file for upload: {}", f.getAbsolutePath());
            ContentType contentType = ContentType.APPLICATION_OCTET_STREAM;
            FileBody fileBody = new FileBody(f, contentType, f.getName());
            mb.addPart("file", fileBody);

            // Some versions expect "inputPackage" as the param name — include both
            mb.addPart("inputPackage", fileBody);
        }

        // Alternative: server-side file reference (file:/... or resource reference)
        if (serverFileRef != null && !serverFileRef.isBlank()) {
            logger.info("Including server-side file reference: {}", serverFileRef);
            // common names used in docs/repo: inputPackage, fileReference, file
            mb.addTextBody("inputPackage", serverFileRef, ContentType.TEXT_PLAIN);
            mb.addTextBody("fileReference", serverFileRef, ContentType.TEXT_PLAIN);
            mb.addTextBody("file", serverFileRef, ContentType.TEXT_PLAIN);
        }

        // Core mapping / recommended parameters (Option B)
        if (wikidocument != null) mb.addTextBody("wikidocument", wikidocument, ContentType.TEXT_PLAIN);
        if (defaultSpace != null) mb.addTextBody("defaultSpace", defaultSpace, ContentType.TEXT_PLAIN);

        // Strategy / behavior
        if (strategy != null) mb.addTextBody("strategy", strategy, ContentType.TEXT_PLAIN);
        // Some implementations use 'replaceDocument' / 'replace' flags — add safe aliases
        mb.addTextBody("replaceDocument", String.valueOf("replace".equalsIgnoreCase(strategy) || "true".equalsIgnoreCase(strategy)), ContentType.TEXT_PLAIN);

        // History / comments / attachments / profiles / unresolved refs
        mb.addTextBody("importHistory", String.valueOf(importHistory), ContentType.TEXT_PLAIN);
        mb.addTextBody("preserveHistory", String.valueOf(importHistory), ContentType.TEXT_PLAIN); // alias
        mb.addTextBody("importComments", String.valueOf(importComments), ContentType.TEXT_PLAIN);
        mb.addTextBody("importAttachments", String.valueOf(importAttachments), ContentType.TEXT_PLAIN);
        mb.addTextBody("importProfiles", String.valueOf(importProfiles), ContentType.TEXT_PLAIN);
        mb.addTextBody("importUnresolvedReferences", String.valueOf(importUnresolvedReferences), ContentType.TEXT_PLAIN);
        mb.addTextBody("importUnresolvedRefs", String.valueOf(importUnresolvedReferences), ContentType.TEXT_PLAIN); // alias

        // Filter stream parameters
        if (filterLogLevel != null) mb.addTextBody("filterLogLevel", filterLogLevel, ContentType.TEXT_PLAIN);
        mb.addTextBody("strictMode", String.valueOf(strictMode), ContentType.TEXT_PLAIN);
        mb.addTextBody("ignoreXmlHeader", String.valueOf(ignoreXmlHeader), ContentType.TEXT_PLAIN);
        mb.addTextBody("cleanEmptySpaces", String.valueOf(cleanEmptySpaces), ContentType.TEXT_PLAIN);

        // Extra safe aliases that some versions expect
        mb.addTextBody("preserveDocIds", String.valueOf(importHistory), ContentType.TEXT_PLAIN);
        mb.addTextBody("preserveSpaceKeys", "true", ContentType.TEXT_PLAIN);
        mb.addTextBody("cleanSpace", String.valueOf(cleanEmptySpaces), ContentType.TEXT_PLAIN);

        // Finalize request
        HttpEntity entity = mb.build();
        post.setEntity(entity);

        // Use a slightly larger timeout for upload/start
        RequestConfig rc = RequestConfig.custom()
                .setConnectTimeout(60_000)
                .setResponseTimeout(60_000)
                .build();
        try (CloseableHttpClient client = HttpClients.custom().setDefaultRequestConfig(rc).build()) {
            var resp = client.execute(post);
            int sc = resp.getCode();
            String body = resp.getEntity() != null ? EntityUtils.toString(resp.getEntity()) : "";

            if (sc >= 200 && sc < 300) {
                logger.info("Import job submitted successfully (HTTP {}). Response: {}", sc, body);
            } else {
                logger.error("Import job submission failed (HTTP {}). Response: {}", sc, body);
                throw new RuntimeException("Import job submission failed: HTTP " + sc + " -> " + body);
            }
        }
    }

    /**
     * Poll the job endpoint until the job has finished or failed.
     *
     * @param jobId   job id used in request URL
     * @param timeout overall timeout duration
     * @param pollIntervalSeconds poll interval seconds
     * @return final job JSON payload (as JsonNode) when finished
     * @throws Exception on timeout or failure
     */
    public JsonNode waitForJobCompletion(String jobId, Duration timeout, int pollIntervalSeconds) throws Exception {
        String url = baseUrl + "/rest/jobs/confluencexmlimport/" + jobId;
        Instant deadline = Instant.now().plus(timeout);

        logger.info("Polling job status at {} (timeout={}s)", url, timeout.toSeconds());

        try (CloseableHttpClient client = HttpClients.createDefault()) {
            while (Instant.now().isBefore(deadline)) {
                HttpGet get = new HttpGet(url);
                get.addHeader("Authorization", basicAuthHeader());
                get.addHeader("Accept", "application/json");

                var resp = client.execute(get);
                int sc = resp.getCode();
                String body = resp.getEntity() != null ? EntityUtils.toString(resp.getEntity()) : "{}";
                if (sc >= 200 && sc < 300) {
                    JsonNode jobJson = mapper.readTree(body);
                    String state = jobJson.path("state").asText(null);
                    if (state == null) {
                        // some versions embed status differently; try 'status'
                        state = jobJson.path("status").asText(null);
                    }
                    logger.info("Job status: {} (raw: {})", state, body);

                    String stLower = (state == null) ? "" : state.toLowerCase();
                    if (stLower.contains("finished") || stLower.contains("success") || stLower.contains("done")) {
                        logger.info("Import job {} finished successfully.", jobId);
                        return jobJson;
                    }
                    if (stLower.contains("failed") || stLower.contains("error") || stLower.contains("cancel")) {
                        logger.error("Import job {} ended with failure: {}", jobId, body);
                        throw new RuntimeException("Import job failed: " + body);
                    }
                } else {
                    logger.warn("Polling returned HTTP {} -> {}", sc, body);
                }

                TimeUnit.SECONDS.sleep(pollIntervalSeconds);
            }
        }

        throw new RuntimeException("Timed out waiting for import job " + jobId + " to finish");
    }
}
