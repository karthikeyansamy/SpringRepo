import org.apache.http.HttpResponse;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.UrlEncodedFormEntity;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.message.BasicNameValuePair;
import org.apache.http.util.EntityUtils;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

public class XwikiImporter {

    private final String baseUrl;
    private final String user;
    private final String pass;

    public XwikiImporter(String baseUrl, String user, String pass) {
        this.baseUrl = baseUrl;
        this.user = user;
        this.pass = pass;
    }

    public void convertAndImportFromServerPath(
            String wiki,
            String space,
            String serverZipPath
    ) throws Exception {

        CloseableHttpClient client = createClient();

        // Step 1: Get form token
        String token = fetchFormToken(client);
        System.out.println("Form token = " + token);

        // Step 2: Call Filter Stream import
        triggerFilterStream(client, wiki, space, serverZipPath, token);

        client.close();
    }

    private CloseableHttpClient createClient() {
        CredentialsProvider provider = new BasicCredentialsProvider();
        provider.setCredentials(AuthScope.ANY,
                new UsernamePasswordCredentials(user, pass));

        return HttpClients.custom()
                .setDefaultCredentialsProvider(provider)
                .build();
    }

    private String fetchFormToken(CloseableHttpClient client) throws Exception {
        String url = baseUrl + "/bin/get/XWiki/XWikiLogin?form_token=true";
        HttpGet get = new HttpGet(url);
        HttpResponse resp = client.execute(get);

        if (resp.getStatusLine().getStatusCode() != 200)
            throw new RuntimeException("Unable to get form token");

        return EntityUtils.toString(resp.getEntity()).trim();
    }

    private void triggerFilterStream(
            CloseableHttpClient client,
            String wiki,
            String space,
            String zipPath,
            String formToken
    ) throws Exception {

        String url = baseUrl + "/bin/filter/stream";

        HttpPost post = new HttpPost(url);

        List<BasicNameValuePair> params = new ArrayList<>();
        params.add(new BasicNameValuePair("form_token", formToken));

        // Input: Confluence XML
        params.add(new BasicNameValuePair("inputSyntax", "confluence+xml"));

        // Output: XWiki instance
        params.add(new BasicNameValuePair("outputSyntax", "xwiki+instance"));

        // SERVER-SIDE input file
        params.add(new BasicNameValuePair("inputSource", "file:" + zipPath));

        // Where to import
        params.add(new BasicNameValuePair("targetWiki", wiki));
        params.add(new BasicNameValuePair("targetSpace", space));

        // Strategy same as UI
        params.add(new BasicNameValuePair("strategy", "add"));

        post.setEntity(new UrlEncodedFormEntity(params, StandardCharsets.UTF_8));

        HttpResponse resp = client.execute(post);
        int code = resp.getStatusLine().getStatusCode();
        String responseBody = EntityUtils.toString(resp.getEntity());

        System.out.println("Response (" + code + "): " + responseBody);

        if (code != 200)
            throw new RuntimeException("Import failed: HTTP " + code);

        System.out.println("Confluence ZIP imported successfully.");
    }
}
