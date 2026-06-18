package com.gotocompany.firehose.sink.http.request.method;

import org.apache.http.client.methods.HttpEntityEnclosingRequestBase;

import java.net.URI;

/**
 * An HTTP {@code DELETE} request that can carry a body.
 *
 * <p>The Apache HttpClient {@code HttpDelete} type does not extend {@code HttpEntityEnclosingRequestBase}
 * and therefore cannot send an entity. This subclass fills that gap by reporting {@code DELETE} as its
 * method while supporting a request body, which Firehose uses when delete bodies are enabled.
 */
public class HttpDeleteWithBody extends HttpEntityEnclosingRequestBase {
    /** The HTTP method name reported by this request. */
    public static final String METHOD_NAME = "DELETE";

    /**
     * Returns the HTTP method name.
     *
     * @return always {@code "DELETE"}
     */
    @Override
    public String getMethod() {
        return METHOD_NAME;
    }

    /**
     * Creates a DELETE request targeting the given URI.
     *
     * @param uri the target URI
     */
    public HttpDeleteWithBody(final URI uri) {
        super();
        setURI(uri);
    }
}
