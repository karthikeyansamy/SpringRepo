package com.migration.xwiki;

import org.apache.hc.client5.http.auth.Credentials;
import org.apache.hc.client5.http.auth.UsernamePasswordCredentials;
import org.apache.hc.client5.http.impl.auth.BasicCredentialsProvider;
import org.apache.hc.client5.http.auth.AuthScope;

public class AuthUtils {
    public static BasicCredentialsProvider basicAuth(String user, String pass) {
        BasicCredentialsProvider provider = new BasicCredentialsProvider();
        Credentials credentials = new UsernamePasswordCredentials(user, pass.toCharArray());
        provider.setCredentials(AuthScope.ANY, credentials);
        return provider;
    }
}
