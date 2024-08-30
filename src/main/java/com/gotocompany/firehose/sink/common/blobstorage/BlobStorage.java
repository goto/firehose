package com.gotocompany.firehose.sink.common.blobstorage;

import com.google.cloud.storage.Blob;

import java.util.List;

/**
 * Abstraction of any storage that store binary bytes as file.
 */
public interface BlobStorage {
    void store(String objectName, String filePath) throws BlobStorageException;

    void store(String objectName, byte[] content) throws BlobStorageException;

    byte[] get(String filePath);

    List<String> list(String prefix);
}
