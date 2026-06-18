package com.gotocompany.firehose.sink.http.auth;

import org.joda.time.DateTimeUtils;

/**
 * Immutable holder for an OAuth2 bearer token and its computed expiry.
 *
 * <p>Created from the {@code access_token} and {@code expires_in} fields returned by an OAuth2 token
 * endpoint and used by {@link OAuth2Credential} to decide when a fresh token must be fetched. When the
 * lifetime is unknown a default of one hour is assumed. The expiry is stored as an absolute timestamp so
 * that {@link #isExpired()} and {@link #getExpiresIn()} reflect elapsed wall-clock time.
 */
public class OAuth2AccessToken {
    /** The raw bearer token value. */
    private final String accessToken;
    /** Absolute epoch time, in milliseconds, at which the token expires. */
    private final Long expirationTimeMs;
    /** Token lifetime, in seconds, assumed when the endpoint does not report one. */
    private static final int DEFAULT_EXPIRATION_TIME = 3600;
    /** Number of milliseconds in one second, used to convert between the two units. */
    private static final long MILLIS = 1000L;

    /**
     * Creates a token, computing its absolute expiry from the supplied lifetime.
     *
     * @param accessToken the raw bearer token value
     * @param expiresIn   the token lifetime in seconds, or {@code null} to assume the default lifetime
     */
    public OAuth2AccessToken(String accessToken, Integer expiresIn) {
        this.accessToken = accessToken;
        expiresIn = expiresIn == null ? DEFAULT_EXPIRATION_TIME : expiresIn;
        this.expirationTimeMs = DateTimeUtils.currentTimeMillis() + (expiresIn * MILLIS);
    }

    /**
     * Reports whether the token has expired or is about to expire within the next minute.
     *
     * @return {@code true} when the remaining lifetime is one minute or less
     */
    public boolean isExpired() {
        final long oneMinute = 60L;
        return this.getExpiresIn() <= oneMinute;
    }

    /**
     * Returns the raw bearer token value.
     *
     * @return the access token string
     */
    public String toString() {
        return this.accessToken;
    }

    /**
     * Returns the remaining token lifetime in seconds.
     *
     * @return the seconds until expiry, which may be negative once the token has expired
     */
    public Long getExpiresIn() {
        return (this.expirationTimeMs - DateTimeUtils.currentTimeMillis()) / MILLIS;
    }
}

