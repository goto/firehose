package com.gotocompany.firehose.sink.blob.writer.local;

import com.google.protobuf.Descriptors;
import com.gotocompany.firehose.config.BlobSinkConfig;
import com.gotocompany.firehose.exception.ConfigurationException;
import com.gotocompany.firehose.metrics.FirehoseInstrumentation;
import com.gotocompany.firehose.sink.blob.writer.local.policy.WriterPolicy;
import lombok.AllArgsConstructor;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import java.util.UUID;

/**
 * Local filesystem abstraction used by the blob sink to create, delete and rotate the files that
 * records are staged into before upload.
 * <p>
 * Builds a {@link LocalFileWriter} for each time-partition under the configured local directory,
 * deletes staged files (and their Parquet {@code .crc} checksum side-files) once they have been
 * uploaded, and decides when a writer should be rotated by applying the configured
 * {@link WriterPolicy} list. Currently only the Parquet writer type is supported.
 *
 * @see LocalFileWriter
 * @see WriterPolicy
 */
@AllArgsConstructor
public class LocalStorage {

    /** Blob sink configuration providing the local directory and writer type. */
    private final BlobSinkConfig sinkConfig;
    /** Protobuf descriptor of the message payload written to each file. */
    private final Descriptors.Descriptor messageDescriptor;
    /** Descriptors of the Kafka metadata fields appended to each record. */
    private final List<Descriptors.FieldDescriptor> metadataFieldDescriptor;
    /** Rotation policies that decide when an open writer should be rotated. */
    private final List<WriterPolicy> policies;
    /** Instrumentation used to log file creation and deletion. */
    private final FirehoseInstrumentation firehoseInstrumentation;

    /**
     * Creates a new local file writer for the given time-partition path.
     * <p>
     * Resolves the partition path under the configured local directory and assigns the file a random
     * UUID name.
     *
     * @param partitionPath the time-partition path the file belongs to
     * @return a new writer for a freshly created local file
     * @throws LocalFileWriterFailedException if the underlying file cannot be created
     * @throws ConfigurationException if the configured file writer type is unsupported
     */
    public LocalFileWriter createLocalFileWriter(Path partitionPath) {
        Path basePath = Paths.get(sinkConfig.getLocalDirectory());
        String fileName = UUID.randomUUID().toString();
        Path dir = basePath.resolve(partitionPath);
        Path fullPath = dir.resolve(Paths.get(fileName));
        return createWriter(basePath, fullPath);
    }

    /**
     * Creates the concrete file writer for the configured writer type.
     *
     * @param basePath the base local directory
     * @param fullPath the full path of the file to create
     * @return a Parquet file writer for the file
     * @throws LocalFileWriterFailedException if the file cannot be created
     * @throws ConfigurationException if the configured file writer type is unsupported
     */
    private LocalParquetFileWriter createWriter(Path basePath, Path fullPath) {
        switch (sinkConfig.getLocalFileWriterType()) {
            case PARQUET:
                try {
                    firehoseInstrumentation.logInfo("Creating Local File " + fullPath);
                    return new LocalParquetFileWriter(
                            System.currentTimeMillis(),
                            basePath.toString(),
                            fullPath.toString(),
                            sinkConfig,
                            messageDescriptor,
                            metadataFieldDescriptor);
                } catch (IOException e) {
                    throw new LocalFileWriterFailedException(e);
                }
            default:
                throw new ConfigurationException("unsupported file writer type");
        }
    }

    /**
     * Deletes a staged local file and its Parquet checksum side-file.
     * <p>
     * Derives the hidden {@code .crc} checksum path that accompanies a Parquet file and removes both.
     *
     * @param pathString the path of the local file to delete
     * @throws LocalFileWriterFailedException if deletion fails
     * @throws ConfigurationException if the configured file writer type is unsupported
     */
    public void deleteLocalFile(String pathString) {
        switch (sinkConfig.getLocalFileWriterType()) {
            case PARQUET:
                try {
                    Path filePath = Paths.get(pathString);
                    Path crcFilePath = filePath.getParent().resolve("." + filePath.getFileName() + ".crc");
                    firehoseInstrumentation.logInfo("Deleting Local File {}", filePath);
                    firehoseInstrumentation.logInfo("Deleting Local File {}", crcFilePath);
                    deleteLocalFile(filePath, crcFilePath);
                } catch (IOException e) {
                    throw new LocalFileWriterFailedException(e);
                }
                break;
            default:
                throw new ConfigurationException("unsupported file writer type");
        }
    }

    /**
     * Deletes the given files from the local filesystem.
     *
     * @param paths the files to delete
     * @throws IOException if any file cannot be deleted
     */
    public void deleteLocalFile(Path... paths) throws IOException {
        for (Path path : paths) {
            Files.delete(path);
        }
    }

    /**
     * Determines whether the given writer should be rotated.
     * <p>
     * Returns {@code true} if any configured {@link WriterPolicy} matches the writer's current
     * metadata, for example its age or size.
     *
     * @param writer the writer to evaluate
     * @return {@code true} if the writer should be rotated, {@code false} otherwise
     */
    public Boolean shouldRotate(LocalFileWriter writer) {
        return policies.stream().anyMatch(writerPolicy -> writerPolicy.shouldRotate(writer.getMetadata()));
    }
}
