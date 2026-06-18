package com.gotocompany.firehose.sink.blob.writer.local;

import com.google.protobuf.Descriptors;
import com.gotocompany.firehose.config.BlobSinkConfig;
import com.gotocompany.firehose.sink.blob.message.Record;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.proto.ProtoParquetWriter;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

/**
 * {@link LocalFileWriter} that writes records to a local Apache Parquet file using a protobuf-backed
 * {@link org.apache.parquet.proto.ProtoParquetWriter}.
 * <p>
 * Each record's message payload is written, optionally alongside its Kafka metadata when metadata
 * output is enabled. Writes are synchronised and a closed writer rejects further records by returning
 * {@code false}. Compression is GZIP and the block and page sizes come from the blob sink
 * configuration.
 *
 * @see LocalStorage
 * @see Record
 */
public class LocalParquetFileWriter implements LocalFileWriter {

    /** Underlying Parquet writer that encodes records to the file. */
    private final ParquetWriter parquetWriter;
    /** Epoch milliseconds at which this writer, and its file, was created. */
    private final long createdTimestampMillis;
    /** Full path of the local Parquet file. */
    private final String fullPath;
    /** Base local directory the file lives under. */
    private final String basePath;
    /** Number of records written so far. */
    private long recordCount = 0;
    /** Whether the writer has been closed. */
    private boolean isClosed = false;
    /** Blob sink configuration controlling metadata inclusion and Parquet sizing. */
    private final BlobSinkConfig sinkConfig;

    /**
     * Creates a Parquet writer for a new local file.
     *
     * @param createdTimestampMillis the file creation time in epoch milliseconds
     * @param basePath the base local directory
     * @param fullPath the full path of the file to write
     * @param sinkConfig the blob sink configuration controlling metadata inclusion and Parquet sizing
     * @param messageDescriptor the protobuf descriptor of the message payload
     * @param metadataFieldDescriptor the descriptors of the Kafka metadata fields
     * @throws IOException if the Parquet writer cannot be created
     */
    public LocalParquetFileWriter(long createdTimestampMillis, String basePath, String fullPath, BlobSinkConfig sinkConfig, Descriptors.Descriptor messageDescriptor, List<Descriptors.FieldDescriptor> metadataFieldDescriptor) throws IOException {
        this.parquetWriter = new ProtoParquetWriter(new Path(fullPath),
                messageDescriptor,
                metadataFieldDescriptor,
                CompressionCodecName.GZIP,
                sinkConfig.getLocalFileWriterParquetBlockSize(),
                sinkConfig.getLocalFileWriterParquetPageSize());
        this.createdTimestampMillis = createdTimestampMillis;
        this.fullPath = fullPath;
        this.basePath = basePath;
        this.sinkConfig = sinkConfig;
    }

    /**
     * Returns the current metadata for the file, including its live data size.
     *
     * @return the file's metadata
     */
    @Override
    public LocalFileMetadata getMetadata() {
        return new LocalFileMetadata(
                basePath,
                fullPath,
                createdTimestampMillis,
                recordCount,
                parquetWriter.getDataSize());
    }

    /**
     * Appends a record to the Parquet file.
     * <p>
     * Writes the message payload, and the Kafka metadata too when metadata output is enabled in the
     * configuration. Does nothing and returns {@code false} if the writer is closed.
     *
     * @param record the record to write
     * @return {@code true} if the record was written, {@code false} if the writer is closed
     * @throws IOException if writing to the Parquet file fails
     */
    public synchronized boolean write(Record record) throws IOException {
        if (isClosed) {
            return false;
        }
        if (sinkConfig.getOutputIncludeKafkaMetadataEnable()) {
            parquetWriter.write(Arrays.asList(record.getMessage(), record.getMetadata()));
        } else {
            parquetWriter.write(record.getMessage());
        }
        recordCount++;
        return true;
    }

    /**
     * Marks the writer closed and closes the underlying Parquet writer.
     *
     * @throws IOException if closing the Parquet writer fails
     */
    @Override
    public synchronized void close() throws IOException {
        this.isClosed = true;
        parquetWriter.close();
    }

    /**
     * Captures the file's metadata and then closes the writer.
     *
     * @return the metadata captured before closing
     * @throws IOException if closing the Parquet writer fails
     */
    @Override
    public synchronized LocalFileMetadata closeAndFetchMetaData() throws IOException {
        LocalFileMetadata metadata = getMetadata();
        this.close();
        return metadata;
    }

    /**
     * Returns the full local path of the Parquet file.
     *
     * @return the file path
     */
    @Override
    public String getFullPath() {
        return fullPath;
    }
}
