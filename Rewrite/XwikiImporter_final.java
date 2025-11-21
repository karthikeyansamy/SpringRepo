package com.example.xwiki;

import org.apache.hc.client5.http.auth.AuthScope;
import org.apache.hc.client5.http.auth.UsernamePasswordCredentials;
import org.apache.hc.client5.http.classic.CloseableHttpClient;
import org.apache.hc.client5.http.classic.methods.HttpPost;
import org.apache.hc.client5.http.impl.auth.BasicCredentialsProvider;
import org.apache.hc.client5.http.impl.classic.HttpClients;
import org.apache.hc.core5.http.NameValuePair;
import org.apache.hc.core5.http.io.entity.UrlEncodedFormEntity;
import org.apache.hc.core5.http.message.BasicNameValuePair;
import org.apache.hc.core5.http.io.entity.EntityUtils;

import java.util.ArrayList;
import java.util.List;

public class XwikiImporter {

    private final String baseUrl;
    private final String username;
    private final String password;

    public XwikiImporter(String baseUrl, String username, String password) {
        this.baseUrl = baseUrl;
        this.username = username;
        this.password = password;
    }

    public void importConfluenceZip(String serverZipPath) throws Exception {

        // REST Job endpoint for filter jobs
        String importUrl = baseUrl + "/rest/wikis/xwiki/jobs?jobType=filter";
        System.out.println("Calling XWiki import endpoint: " + importUrl);

        // Credentials
        BasicCredentialsProvider credsProvider = new BasicCredentialsProvider();
        credsProvider.setCredentials(
                new AuthScope(null, -1),
                new UsernamePasswordCredentials(username, password.toCharArray())
        );

        try (CloseableHttpClient client = HttpClients.custom()
                .setDefaultCredentialsProvider(credsProvider)
                .build()) {

            HttpPost post = new HttpPost(importUrl);

            // Filter application parameters
            List<NameValuePair> params = new ArrayList<>();

            // Input from Confluence ZIP
            params.add(new BasicNameValuePair("inputFilter", "confluence+xml"));
            params.add(new BasicNameValuePair("inputFile", serverZipPath));

            // Output into XWiki instance
            params.add(new BasicNameValuePair("outputFilter", "xwiki+instance"));
            params.add(new BasicNameValuePair("outputTarget", "xwiki:wiki:xwiki"));

            // Optional advanced settings
            params.add(new BasicNameValuePair("preserveTimestamps", "true"));
            params.add(new BasicNameValuePair("preserveSpaceNames", "true"));
            params.add(new BasicNameValuePair("preserveUserNames", "true"));
            params.add(new BasicNameValuePair("preserveHistory", "true"));
            params.add(new BasicNameValuePair("importAttachments", "true"));
            params.add(new BasicNameValuePair("importComments", "true"));
            params.add(new BasicNameValuePair("verbose", "true"));

            post.setEntity(new UrlEncodedFormEntity(params));

            String response = client.execute(post,
                    httpResponse -> EntityUtils.toString((httpResponse).getEntity()));

            System.out.println("=== XWiki Import Job Response ===");
            System.out.println(response);
        }
    }
}
