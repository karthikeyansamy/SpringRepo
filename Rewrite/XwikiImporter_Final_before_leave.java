package com.migration.xwiki;

import org.apache.hc.client5.http.auth.AuthScope;
import org.apache.hc.client5.http.auth.UsernamePasswordCredentials;
import org.apache.hc.client5.http.classic.methods.HttpGet;
import org.apache.hc.client5.http.classic.methods.HttpPost;
import org.apache.hc.client5.http.entity.mime.HttpMultipartMode;
import org.apache.hc.client5.http.entity.mime.MultipartEntityBuilder;
import org.apache.hc.client5.http.impl.classic.CloseableHttpClient;
import org.apache.hc.client5.http.impl.classic.HttpClientBuilder;
import org.apache.hc.client5.http.impl.auth.BasicCredentialsProvider;
import org.apache.hc.core5.http.ClassicHttpResponse;
import org.apache.hc.core5.http.Header;
import org.apache.hc.client5.http.impl.classic.HttpClients;
import org.apache.hc.core5.http.io.entity.EntityUtils;

import java.io.File;

public class XwikiImporter {

    /**
     * Fetch form token from the custom endpoint:
     *   /bin/view/Token/Header?outputSyntax=plain
     *
     * This page must contain:
     *
     *  #set($token = $xwiki.getFormToken())
     *  #set($discard = $response.setHeader("xwiki-form-token", $token))
     *  OK
     *
     * This call DOES NOT use authentication.
     */
    public String fetchTokenFromHeader(String baseUrl) throws Exception {

        String url = baseUrl + "/bin/view/Token/Header?outputSyntax=plain";

        try (CloseableHttpClient client = HttpClients.createDefault()) {

            HttpGet get = new HttpGet(url);

            try (ClassicHttpResponse resp = client.executeOpen(null, get, null)) {

                Header tokenHeader = resp.getHeader("xwiki-form-token");
                if (tokenHeader == null) {
                    throw new RuntimeException("Missing xwiki-form-token header. Check Token/Header page setup.");
                }

                return tokenHeader.getValue();
            }
        }
    }


    /**
     * POST /bin/filter/stream with basic authentication.
     *
     * @param baseUrl XWiki base URL
     * @param serverZipFilePath Path to Confluence ZIP located ON SERVER
     * @param user XWiki username
     * @param pass XWiki password
     */
    public void triggerImport(String baseUrl, String serverZipFilePath,
                              String user, String pass) throws Exception {

        // Fetch form token (no auth)
        String token = fetchTokenFromHeader(baseUrl);
        System.out.println("Fetched token = " + token);

        String url = baseUrl + "/bin/filter/stream";

        // Build authenticated client
        BasicCredentialsProvider creds = new BasicCredentialsProvider();
        creds.setCredentials(
                new AuthScope(null, -1),
                new UsernamePasswordCredentials(user, pass.toCharArray())
        );

        CloseableHttpClient client = HttpClientBuilder.create()
                .setDefaultCredentialsProvider(creds)
                .build();

        // Build multipart request
        MultipartEntityBuilder builder = MultipartEntityBuilder.create();
        builder.setMode(HttpMultipartMode.STRICT);

        builder.addTextBody("form_token", token);
        builder.addTextBody("inputSyntax", "confluence+xml");
        builder.addTextBody("outputSyntax", "xwiki+xml");

        File serverFile = new File(serverZipFilePath);
        if (!serverFile.exists()) {
            throw new RuntimeException("ZIP does not exist on server path: " + serverZipFilePath);
        }

        builder.addBinaryBody("input", serverFile);

        HttpPost post = new HttpPost(url);
        post.setEntity(builder.build());

        // Execute import
        try (ClassicHttpResponse resp = client.executeOpen(null, post, null)) {
            System.out.println("Import STATUS = " + resp.getCode());
            System.out.println("Response:");
            System.out.println(EntityUtils.toString(resp.getEntity()));
        }
    }


    public static void main(String[] args) throws Exception {

        if (args.length < 4) {
            System.out.println("Usage: java XwikiImporter <baseUrl> <serverZipPath> <user> <pass>");
            return;
        }

        String baseUrl = args[0];
        String zipPath = args[1];
        String user = args[2];
        String pass = args[3];

        XwikiImporter imp = new XwikiImporter();
        imp.triggerImport(baseUrl, zipPath, user, pass);
    }
}
