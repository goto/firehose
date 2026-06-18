package com.gotocompany.firehose.sink.common.blobstorage;

/**
 * Abstraction of any storage that store binary bytes as file.
 */
public interface BlobStorage {
    /**
     * Uploads the contents of a local file under the given object name.
     *
     * @param objectName the destination object name (key) within the store
     * @param filePath   the path of the local file to upload
     * @throws BlobStorageException if the file cannot be read or the upload fails
     */
    void store(String objectName, String filePath) throws BlobStorageException;

    /**
     * Uploads the given bytes under the given object name.
     *
     * @param objectName the destination object name (key) within the store
     * @param content    the bytes to upload
     * @throws BlobStorageException if the upload fails
     */
    void store(String objectName, byte[] content) throws BlobStorageException;
}
