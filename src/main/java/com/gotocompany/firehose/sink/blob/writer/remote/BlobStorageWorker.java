package com.gotocompany.firehose.sink.blob.writer.remote;

import com.gotocompany.firehose.sink.common.blobstorage.BlobStorage;
import com.gotocompany.firehose.sink.common.blobstorage.BlobStorageException;
import com.gotocompany.firehose.sink.blob.writer.local.LocalFileMetadata;
import lombok.AllArgsConstructor;

import java.nio.file.Paths;
import java.time.Duration;
import java.time.Instant;
import java.util.concurrent.Callable;

/**
 * Uploads a local file to object-storage and returns the total time taken.
 */
@AllArgsConstructor
public class BlobStorageWorker implements Callable<Long> {

    /** The blob storage backend the file is uploaded to. */
    private final BlobStorage blobStorage;
    /** Metadata describing the local file to upload. */
    private final LocalFileMetadata metadata;

    /**
     * Uploads the file and returns how long the upload took.
     *
     * @return the upload duration in milliseconds
     * @throws BlobStorageException if storing the file in blob storage fails
     */
    @Override
    public Long call() throws BlobStorageException {
        Instant start = Instant.now();
        String objectName = Paths.get(metadata.getBasePath()).relativize(Paths.get(metadata.getFullPath())).toString();
        blobStorage.store(objectName, metadata.getFullPath());
        return Duration.between(start, Instant.now()).toMillis();
    }
}
