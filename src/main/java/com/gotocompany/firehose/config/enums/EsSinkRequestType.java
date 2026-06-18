package com.gotocompany.firehose.config.enums;

/**
 * Determines how the Elasticsearch sink writes each message into the target index.
 *
 * <p>The value is not configured directly; the Elasticsearch request handler factory derives it
 * from the update-only flag on the Elasticsearch sink configuration and uses it to pick between an
 * update and an upsert request handler.
 */
public enum EsSinkRequestType {
    /**
     * Only updates documents that already exist; messages with no matching document are not indexed.
     */
    UPDATE_ONLY,
    /**
     * Updates a document when it already exists and indexes a new one otherwise (upsert).
     */
    INSERT_OR_UPDATE
}
