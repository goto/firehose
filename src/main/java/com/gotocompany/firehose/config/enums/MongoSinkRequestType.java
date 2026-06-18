package com.gotocompany.firehose.config.enums;

/**
 * Determines how the MongoDB sink writes each message into the target collection.
 *
 * <p>The value is not configured directly; the MongoDB request handler factory derives it from the
 * update-only flag on the MongoDB sink configuration and uses it to pick between an update and an
 * upsert request handler.
 */
public enum MongoSinkRequestType {
    /**
     * Only updates documents that already exist; messages whose primary key has no matching
     * document are not inserted. A primary key must be configured for this mode.
     */
    UPDATE_ONLY,
    /**
     * Updates a document when it already exists and inserts a new one otherwise (upsert).
     */
    UPSERT
}
