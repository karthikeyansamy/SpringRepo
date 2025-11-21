public class XWikiImporter {

    private final String baseUrl;
    private final String user;
    private final String pass;

    public XWikiImporter(String baseUrl, String user, String pass) {
        this.baseUrl = baseUrl;
        this.user = user;
        this.pass = pass;
    }

    public void triggerImport(String wiki, String space, String zipPath) throws Exception {

        String importUrl = baseUrl + "/rest/wikis/" + wiki + "/jobs/import";

        CloseableHttpClient client = HttpClients.custom()
                .setDefaultCredentialsProvider(credentials())
                .build();

        HttpPost post = new HttpPost(importUrl);

        MultipartEntityBuilder builder = MultipartEntityBuilder.create();
        builder.addTextBody("jobType", "import");
        builder.addTextBody("inputSyntax", "confluence+xml");
        builder.addTextBody("targetWiki", wiki);
        builder.addTextBody("targetSpace", space);
        builder.addTextBody("inputSource", "file:" + zipPath);

        File f = new File(zipPath);
        builder.addBinaryBody("file", f, ContentType.DEFAULT_BINARY, f.getName());

        post.setEntity(builder.build());

        CloseableHttpResponse resp = client.execute(post);
        int code = resp.getStatusLine().getStatusCode();

        if (code >= 200 && code < 300) {
            System.out.println("Import job submitted successfully");
        } else {
            System.err.println("Import failed: HTTP " + code);
            System.err.println(EntityUtils.toString(resp.getEntity()));
        }

        resp.close();
        client.close();
    }

    private CredentialsProvider credentials() {
        CredentialsProvider provider = new BasicCredentialsProvider();
        provider.setCredentials(AuthScope.ANY,
                new UsernamePasswordCredentials(user, pass));
        return provider;
    }
}
