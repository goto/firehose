package com.gotocompany.firehose.sink.http.request.create;

import com.gotocompany.firehose.message.Message;
import com.gotocompany.firehose.sink.http.request.entity.RequestEntityBuilder;
import org.apache.http.client.methods.HttpEntityEnclosingRequestBase;

import java.net.URISyntaxException;
import java.util.List;

/**
 * Creates http requests.
 */
public interface RequestCreator {

    /**
     * Builds the HTTP requests for the given messages.
     *
     * @param bodyContents the messages whose serialized bodies populate the requests
     * @param entity       builder that wraps a serialized body into an HTTP entity
     * @return the list of requests to send
     * @throws URISyntaxException if a request URI cannot be built
     */
    List<HttpEntityEnclosingRequestBase> create(List<Message> bodyContents, RequestEntityBuilder entity) throws URISyntaxException;
}
