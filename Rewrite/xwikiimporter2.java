import org.apache.hc.client5.http.classic.methods.HttpGet;
import org.apache.hc.client5.http.classic.methods.HttpPost;
import org.apache.hc.client5.http.config.RequestConfig;
import org.apache.hc.client5.http.impl.classic.CloseableHttpClient;
import org.apache.hc.client5.http.impl.classic.HttpClients;
import org.apache.hc.client5.http.entity.UrlEncodedFormEntity;
import org.apache.hc.core5.http.io.entity.EntityUtils;
import org.apache.hc.core5.http.NameValuePair;
import org.apache.hc.core5.http.message.BasicNameValuePair;

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

    /**
     * 1) Get form token (works for all XWiki versions)
     */
    private String getFormToken() throws Exception {

        String tokenUrl = baseUrl + "/bin/getauthenticator";

        RequestConfig config = RequestConfig.custom()
                .setRedirectsEnabled(true)
                .build();

        HttpGet get = new HttpGet(tokenUrl);
        get.addHeader("X-Requested-With", "XMLHttpRequest");
        get.addHeader("Authorization", basicAuth());

        try (CloseableHttpClient client = HttpClients.custom()
                .setDefaultRequestConfig(config)
                .build()) {

            return EntityUtils.toString(client.execute(get).getEntity()).trim();
        }
    }

    /**
     * 2) Submit Filter Application Job
     */
    public void triggerConfluenceImport(String filePathOnServer) throws Exception {

        String token = getFormToken();
        if (token == null || token.isEmpty()) {
            throw new RuntimeException("Failed to obtain XWiki form token");
        }

        System.out.println("Form token = " + token);

        String jobUrl = baseUrl + "/rest/jobs/filter";

        HttpPost post = new HttpPost(jobUrl);
        post.addHeader("Authorization", basicAuth());
        post.addHeader("Content-Type", "application/x-www-form-urlencoded");

        List<NameValuePair> params = new ArrayList<>();
        params.add(new BasicNameValuePair("jobType", "filter"));

        // Input Confluence XML Stream → MUST MATCH extension docs
        params.add(new BasicNameValuePair("inputFilterStreamFactoryId", "confluence+xml"));
        params.add(new BasicNameValuePair("inputSource", filePathOnServer));

        // Output to XWiki instance → import pages/attachments/comments/etc.
        params.add(new BasicNameValuePair("outputFilterStreamFactoryId", "xwiki+instance"));

        params.add(new BasicNameValuePair("interactive", "false"));

        // Mandatory token
        params.add(new BasicNameValuePair("form_token", token));

        post.setEntity(new UrlEncodedFormEntity(params));

        try (CloseableHttpClient client = HttpClients.createDefault()) {
            var response = client.execute(post);
            String body = EntityUtils.toString(response.getEntity());

            System.out.println("Status: " + response.getCode());
            System.out.println(body);
        }
    }

    private String basicAuth() {
        String auth = username + ":" + password;
        return "Basic " + java.util.Base64.getEncoder().encodeToString(auth.getBytes());
    }

    // MAIN for testing
    public static void main(String[] args) throws Exception {

        String base = "https://wiki.wocommunity.org/xwiki";
        String user = "Admin";
        String pass = "admin";

        XwikiImporter importer = new XwikiImporter(base, user, pass);

        importer.triggerConfluenceImport("/xwiki/migration/test_space.zip");
    }
}
