package com.gotocompany.firehose.sink.http.auth;

import com.gotocompany.firehose.exception.OAuth2Exception;
import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;

import java.io.IOException;
import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;

import org.apache.http.HttpResponse;
import org.apache.http.NameValuePair;
import org.apache.http.client.HttpClient;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.entity.UrlEncodedFormEntity;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.message.BasicNameValuePair;
import org.apache.http.util.EntityUtils;

/**
 * Minimal OAuth2 client that obtains access tokens using the client-credentials grant.
 *
 * <p>Used by {@link OAuth2Credential} to fetch a token from the configured token endpoint. The client
 * posts the client id, secret and scope as a form-encoded body, parses the JSON response and returns an
 * {@link OAuth2AccessToken}. A short fixed timeout is applied to the token request, and any non-2xx
 * response is surfaced as an {@link com.gotocompany.firehose.exception.OAuth2Exception}.
 */
public class OAuth2Client {
    /** HTTP client dedicated to the token endpoint, configured with a short timeout. */
    private final HttpClient client;
    /** OAuth2 client identifier sent with the token request. */
    private final String clientId;
    /** OAuth2 client secret sent with the token request. */
    private final String clientSecret;
    /** Space-delimited scopes requested for the token. */
    private final String scope;
    /** URL of the OAuth2 token endpoint. */
    private final String accessTokenEndpoint;
    /** Connect, socket and connection-request timeout, in milliseconds, for the token request. */
    private final int timeoutMs = 5000;
    /** Regular expression matching 2xx HTTP status codes that denote a successful token response. */
    private static final String SUCCESS_CODE_PATTERN = "^2.*";

    /**
     * Creates a client bound to a token endpoint and the credentials used to authenticate against it.
     *
     * @param clientId            the OAuth2 client identifier
     * @param clientSecret        the OAuth2 client secret
     * @param scope               the space-delimited scopes to request
     * @param accessTokenEndpoint the URL of the token endpoint
     */
    public OAuth2Client(String clientId, String clientSecret, String scope, String accessTokenEndpoint) {
        this.clientId = clientId;
        this.clientSecret = clientSecret;
        this.scope = scope;
        this.accessTokenEndpoint = accessTokenEndpoint;
        this.client = this.httpClient();
    }

    /**
     * Builds the closeable HTTP client used for token requests, applying the fixed request timeouts.
     *
     * @return a configured HTTP client
     */
    private CloseableHttpClient httpClient() {
        RequestConfig config = RequestConfig.custom().setConnectTimeout(timeoutMs).setConnectionRequestTimeout(timeoutMs).setSocketTimeout(timeoutMs).build();
        return HttpClientBuilder.create().setDefaultRequestConfig(config).build();
    }

    /**
     * Requests a new access token from the endpoint using the client-credentials grant.
     *
     * <p>The client id, secret, scope and {@code grant_type} are posted as a form-encoded body and the
     * JSON response is parsed into an {@link OAuth2AccessToken}.
     *
     * @return the access token returned by the endpoint
     * @throws IOException     if the token request cannot be executed or its response read
     * @throws OAuth2Exception if the endpoint returns a non-2xx response
     */
    public OAuth2AccessToken requestClientCredentialsGrantAccessToken() throws IOException {
        HttpPost req = new HttpPost(this.accessTokenEndpoint);
        req.setHeader("Content-Type", "application/x-www-form-urlencoded");
        List<NameValuePair> kv = new ArrayList();
        kv.add(new BasicNameValuePair("client_id", this.clientId));
        kv.add(new BasicNameValuePair("client_secret", this.clientSecret));
        kv.add(new BasicNameValuePair("scope", this.scope));
        kv.add(new BasicNameValuePair("grant_type", "client_credentials"));
        req.setEntity(new UrlEncodedFormEntity(kv, "UTF-8"));
        HttpResponse response = this.client.execute(req);
        String body = EntityUtils.toString(response.getEntity());
        Type responseMapType = (new TypeToken<Map<String, String>>() {
        }).getType();
        Map<String, String> map = new Gson().fromJson(body, responseMapType);

        if (!Pattern.compile(SUCCESS_CODE_PATTERN).matcher(String.valueOf(response.getStatusLine().getStatusCode())).matches()) {
            throw new OAuth2Exception("OAuthException: " + map.get("error"));
        } else {
            String accessToken = map.get("access_token");
            String expiresInRaw = map.get("expires_in");
            Integer expiresIn = expiresInRaw == null ? null : Integer.valueOf(expiresInRaw);
            return new OAuth2AccessToken(accessToken, expiresIn);
        }
    }
}
