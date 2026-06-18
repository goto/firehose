package com.gotocompany.firehose.sink.blob;

import com.gotocompany.firehose.consumer.kafka.OffsetManager;
import com.gotocompany.firehose.exception.DeserializerException;
import com.gotocompany.firehose.exception.EmptyMessageException;
import com.gotocompany.firehose.exception.SinkException;
import com.gotocompany.firehose.exception.UnknownFieldsException;
import com.gotocompany.firehose.message.Message;
import com.gotocompany.firehose.metrics.FirehoseInstrumentation;
import com.gotocompany.firehose.sink.blob.message.MessageDeSerializer;
import com.gotocompany.firehose.sink.blob.message.Record;
import com.gotocompany.firehose.sink.blob.writer.WriterOrchestrator;
import com.gotocompany.depot.error.ErrorInfo;
import com.gotocompany.depot.error.ErrorType;
import com.gotocompany.firehose.sink.AbstractSink;

import java.io.IOException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

/**
 * Firehose sink that writes consumed Kafka messages to blob storage (GCS, S3, OSS or COS) as
 * time-partitioned Parquet files.
 * <p>
 * Each message is deserialized into a {@link Record} by {@link MessageDeSerializer} and handed to a
 * {@link WriterOrchestrator}, which appends it to the correct local file for its time partition and
 * asynchronously uploads completed files to the configured blob storage. The sink tracks which
 * messages were written to which file so that Kafka offsets are only committed once their file has
 * been flushed remotely.
 * <p>
 * Unlike most sinks, the blob sink manages Kafka offsets itself: {@link #canManageOffsets()} returns
 * {@code true}, {@link #addOffsetsAndSetCommittable(List)} records offsets per batch, and
 * {@link #calculateCommittableOffsets()} marks the offsets of remotely flushed files as committable.
 * Per-message deserialization problems (empty messages, unknown fields, deserialization errors) are
 * recorded as failed messages with the matching error type rather than failing the whole batch,
 * while any other error is wrapped in a {@link SinkException}.
 * <p>
 * Configured by {@link com.gotocompany.firehose.config.BlobSinkConfig} and created by
 * {@link BlobSinkFactory}.
 *
 * @see AbstractSink
 * @see WriterOrchestrator
 */
public class BlobSink extends AbstractSink {

    /** Routes records to the correct time-partitioned local file and uploads completed files to blob storage. */
    private final WriterOrchestrator writerOrchestrator;
    /** Tracks Kafka offsets per output file so they can be committed once the file is flushed remotely. */
    private final OffsetManager offsetManager;
    /** Converts incoming Kafka {@link Message} objects into {@link Record} instances for writing. */
    private final MessageDeSerializer messageDeSerializer;

    /** The current batch staged by {@link #prepare(List)} and consumed by {@link #execute()}. */
    private List<Message> messages;

    /**
     * Creates a blob sink with its writer orchestrator, offset manager and deserializer.
     *
     * @param firehoseInstrumentation the instrumentation used for logging and metrics
     * @param sinkType the sink type name used to tag telemetry
     * @param offsetManager the offset manager that tracks committable offsets per output file
     * @param writerOrchestrator the orchestrator that writes records to local files and uploads them
     * @param messageDeSerializer the deserializer that converts messages into records
     */
    public BlobSink(FirehoseInstrumentation firehoseInstrumentation, String sinkType, OffsetManager offsetManager, WriterOrchestrator writerOrchestrator, MessageDeSerializer messageDeSerializer) {
        super(firehoseInstrumentation, sinkType);
        this.offsetManager = offsetManager;
        this.writerOrchestrator = writerOrchestrator;
        this.messageDeSerializer = messageDeSerializer;
    }

    /**
     * Deserializes and writes the staged batch, returning the messages that failed.
     * <p>
     * Each message is converted to a {@link Record} and written through the
     * {@link WriterOrchestrator}; the message is then associated with the local file path it was
     * written to. Messages that fail deserialization are tagged with the appropriate
     * {@link ErrorType} (invalid message, unknown fields or deserialization error) and added to the
     * failed list, while any other failure is rethrown as a {@link SinkException}. Finally the
     * file-to-messages mapping is registered with the offset manager so offsets can be committed when
     * their files are flushed.
     *
     * @return the messages that could not be deserialized or written
     * @throws Exception if writing fails unrecoverably, typically wrapped in a {@link SinkException}
     */
    @Override
    protected List<Message> execute() throws Exception {
        List<Message> failedMessages = new LinkedList<>();
        Map<Object, List<Message>> fileToMessages = new HashMap<>();
        for (Message message : messages) {
            try {
                Record record = messageDeSerializer.deSerialize(message);
                String filePath = writerOrchestrator.write(record);
                fileToMessages.computeIfAbsent(filePath, key -> new ArrayList<>()).add(message);
            } catch (EmptyMessageException e) {
                getFirehoseInstrumentation().logWarn("empty message found on topic: {}, partition: {}, offset: {}",
                        message.getTopic(), message.getPartition(), message.getOffset());
                message.setErrorInfo(new ErrorInfo(e, ErrorType.INVALID_MESSAGE_ERROR));
                failedMessages.add(message);
            } catch (UnknownFieldsException e) {
                getFirehoseInstrumentation().logWarn(e.getMessage());
                message.setErrorInfo(new ErrorInfo(e, ErrorType.UNKNOWN_FIELDS_ERROR));
                failedMessages.add(message);
            } catch (DeserializerException e) {
                getFirehoseInstrumentation().logWarn("message deserialization failed on topic: {}, partition: {}, offset: {}, reason: {}",
                        message.getTopic(), message.getPartition(), message.getOffset(), e.getMessage());
                message.setErrorInfo(new ErrorInfo(e, ErrorType.DESERIALIZATION_ERROR));
                failedMessages.add(message);
            } catch (Exception e) {
                throw new SinkException("Failed to deserialize the message", e);
            }
        }
        offsetManager.addOffsetToBatch(fileToMessages);
        return failedMessages;
    }

    /**
     * Stages the batch to be written by {@link #execute()}.
     *
     * @param messageList the batch of messages to write
     * @throws IOException if an I/O error occurs
     * @throws SQLException if a database error occurs
     */
    @Override
    protected void prepare(List<Message> messageList) throws IOException, SQLException {
        this.messages = messageList;
    }

    /**
     * Closes the {@link WriterOrchestrator}, flushing and shutting down its writer threads.
     *
     * @throws IOException if closing the orchestrator fails
     */
    @Override
    public void close() throws IOException {
        writerOrchestrator.close();
    }

    /**
     * Marks the offsets of all remotely flushed files as ready to be committed.
     * <p>
     * Drains the paths that the {@link WriterOrchestrator} has flushed to blob storage and sets each
     * one committable on the offset manager.
     */
    @Override
    public void calculateCommittableOffsets() {
        writerOrchestrator.getFlushedPaths().forEach(offsetManager::setCommittable);
    }

    /**
     * Indicates that the blob sink manages its own Kafka offsets.
     *
     * @return always {@code true}
     */
    @Override
    public boolean canManageOffsets() {
        return true;
    }

    /**
     * Records the offsets of the given messages with the offset manager.
     *
     * @param messageList the messages whose offsets should be tracked
     */
    @Override
    public void addOffsetsAndSetCommittable(List<Message> messageList) {
        offsetManager.addOffsetsAndSetCommittable(messageList);
    }
}
