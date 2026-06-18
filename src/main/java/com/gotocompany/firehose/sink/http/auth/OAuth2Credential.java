package com.gotocompany.firehose.sink.http.auth;

import com.gotocompany.firehose.metrics.FirehoseInstrumentation;
import okhttp3.Interceptor;
import okhttp3.Request;
import okhttp3.Response;
import org.apache.http.HttpRequestInterceptor;
import org.apache.http.HttpResponseInterceptor;
import org.apache.http.HttpStatus;
import org.apache.http.impl.client.HttpClientBuilder;

import java.io.IOException;

/**
 * Interceptor to add oauth token in http request.
 */
public class OAuth2Credential implements Interceptor {

    /** Token endpoint client used to fetch fresh access tokens. */
    private final OAuth2Client client;
    /** Currently cached access token, or {@code null} when none has been fetched yet. */
    private OAuth2AccessToken accessToken;
    /** Instrumentation used to log token lifecycle events. */
    private FirehoseInstrumentation firehoseInstrumentation;

    /**
     * Creates a credential that fetches tokens from the given endpoint with the supplied client details.
     *
     * @param firehoseInstrumentation instrumentation used to log token activity
     * @param clientId                the OAuth2 client identifier
     * @param clientSecret            the OAuth2 client secret
     * @param scope                   the space-delimited scopes to request
     * @param accessTokenEndpoint     the URL of the OAuth2 token endpoint
     */
    public OAuth2Credential(FirehoseInstrumentation firehoseInstrumentation, String clientId, String clientSecret, String scope, String accessTokenEndpoint) {
        this.firehoseInstrumentation = firehoseInstrumentation;
        this.client = new OAuth2Client(clientId, clientSecret, scope, accessTokenEndpoint);
    }

    /**
     * Fetches a new access token from the endpoint and caches it.
     *
     * @throws IOException if the token cannot be retrieved
     */
    public void requestAccessToken() throws IOException {
        firehoseInstrumentation.logInfo("Requesting Access Token, expires in: {0}",
                (this.accessToken == null ? "<none>" : this.accessToken.getExpiresIn()));
        OAuth2AccessToken token = client.requestClientCredentialsGrantAccessToken();
        setAccessToken(token);
    }

    /**
     * Builds the Apache HttpClient request interceptor that attaches the bearer token.
     *
     * <p>The interceptor refreshes the token when it is missing or expired and adds an
     * {@code Authorization: Bearer} header; a failure to obtain a token is logged and the request proceeds
     * without the header.
     *
     * @return the request interceptor
     */
    public HttpRequestInterceptor requestInterceptor() {
        return (request, context) -> {
            try {
                if (getAccessToken() == null || getAccessToken().isExpired()) {
                    requestAccessToken();
                }
                request.addHeader("Authorization", "Bearer " + getAccessToken().toString());
            } catch (IOException e) {
                firehoseInstrumentation.logWarn("OAuth2 request access token failed: {0}", e.getMessage());
            }
        };
    }

    /**
     * Builds the Apache HttpClient response interceptor that invalidates the token on authentication failure.
     *
     * @return the response interceptor that clears the cached token on a 401 response
     */
    public HttpResponseInterceptor responseInterceptor() {
        return (response, context) -> {
            boolean isTokenExpired = response.getStatusLine().getStatusCode() == HttpStatus.SC_UNAUTHORIZED;
            if (isTokenExpired) {
                setAccessToken(null);
            }
        };
    }

    /**
     * Registers the request and response interceptors on an Apache HttpClient builder.
     *
     * @param builder the client builder to augment
     * @return the same builder with the OAuth2 interceptors installed
     */
    public HttpClientBuilder initialize(HttpClientBuilder builder) {
        return builder.addInterceptorFirst(this.requestInterceptor()).addInterceptorLast(this.responseInterceptor());
    }

    /**
     * Returns the currently cached access token.
     *
     * @return the cached token, or {@code null} when none has been fetched
     */
    public OAuth2AccessToken getAccessToken() {
        return accessToken;
    }

    /**
     * Replaces the cached access token.
     *
     * @param accessToken the token to cache, or {@code null} to force a refresh on the next request
     */
    public void setAccessToken(OAuth2AccessToken accessToken) {
        this.accessToken = accessToken;
    }

    /**
     * OkHttp interceptor entry point that attaches the bearer token and refreshes it on a 401 response.
     *
     * @param chain the OkHttp interceptor chain
     * @return the response produced by proceeding along the chain
     * @throws IOException if the underlying call fails
     */
    @Override
    public Response intercept(Chain chain) throws IOException {
        Request request = chain.request();
        try {
            if (getAccessToken() == null || getAccessToken().isExpired()) {
                requestAccessToken();
            }
            request = request.newBuilder().header("Authorization", "Bearer " + getAccessToken().toString()).build();
        } catch (IOException e) {
            firehoseInstrumentation.logWarn("OAuth2 request access token failed: {0}", e.getMessage());
        }

        Response response = chain.proceed(request);
        boolean isTokenExpired = response.code() == HttpStatus.SC_UNAUTHORIZED;
        if (isTokenExpired) {
            setAccessToken(null);
        }
        return response;
    }
}

