package com.gotocompany.firehose.sink.http.request.uri;

import com.gotocompany.firehose.config.enums.HttpSinkParameterSourceType;
import com.gotocompany.firehose.message.Message;
import com.gotocompany.firehose.proto.ProtoToFieldMapper;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.Map;

/**
 * Builds URI based on the requirement.
 */
public class UriBuilder {

    /** Configured service URL, used directly for batched requests. */
    private String baseURL;
    /** Parser that renders per-message URLs from the base URL template. */
    private UriParser uriParser;
    /** Mapper supplying query parameters for parameterized URIs, or {@code null} when not parameterized. */
    private ProtoToFieldMapper protoToFieldMapper;
    /** Source of the parameter values (message key or body) for parameterized URIs. */
    private HttpSinkParameterSourceType httpSinkParameterSourceType;

    /**
     * Creates a URI builder for a base URL and parser.
     *
     * @param baseURL   the configured service URL
     * @param uriParser the parser used to render per-message URLs
     */
    public UriBuilder(String baseURL, UriParser uriParser) {
        this.baseURL = baseURL;
        this.uriParser = uriParser;
    }

    /**
     * Builds the static base URI, used for batched requests.
     *
     * @return the base URL as a URI
     * @throws URISyntaxException if the base URL is not a valid URI
     */
    public URI build() throws URISyntaxException {
        return new URI(baseURL);
    }

    /**
     * Builds the URI for a single message, adding proto-derived query parameters when parameterized.
     *
     * @param message the message whose payload renders the URL and supplies any parameters
     * @return the rendered URI
     * @throws URISyntaxException if the rendered URL is not a valid URI
     */
    public URI build(Message message) throws URISyntaxException {
        String url = uriParser.parse(message, baseURL);
        org.apache.http.client.utils.URIBuilder uriBuilder = new org.apache.http.client.utils.URIBuilder(url);
        if (protoToFieldMapper == null) {
            return uriBuilder.build();
        }

        // flow for parameterized URI
        Map<String, Object> paramMap = protoToFieldMapper
                .getFields((httpSinkParameterSourceType == HttpSinkParameterSourceType.KEY) ? message.getLogKey()
                        : message.getLogMessage());
        paramMap.forEach((string, object) -> uriBuilder.addParameter(string, object.toString()));
        return uriBuilder.build();
    }

    /**
     * Enables parameterized URIs by supplying the field mapper and parameter source.
     *
     * @param protoToFieldmapper      mapper that extracts the proto fields appended as query parameters
     * @param httpSinkParameterSource source of the parameter values (message key or body)
     * @return this builder, configured for parameterized URIs
     */
    public UriBuilder withParameterizedURI(ProtoToFieldMapper protoToFieldmapper, HttpSinkParameterSourceType httpSinkParameterSource) {
        this.protoToFieldMapper = protoToFieldmapper;
        this.httpSinkParameterSourceType = httpSinkParameterSource;
        return this;
    }
}
