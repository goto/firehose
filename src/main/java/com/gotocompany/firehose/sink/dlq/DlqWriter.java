package com.gotocompany.firehose.sink.dlq;

import com.gotocompany.firehose.message.Message;

import java.io.IOException;
import java.util.List;

/**
 * Contract for writing messages that Firehose could not deliver to a dead letter queue (DLQ).
 * <p>
 * A DLQ writer is the fallback destination for messages a sink failed to process. Firehose supports
 * several implementations, selected by configuration: a Kafka retry topic, blob storage, or the
 * application log. Each {@link #write(List)} call returns the messages that even the DLQ failed to
 * accept.
 *
 * @see DlqWriterFactory
 * @see com.gotocompany.firehose.sink.dlq.kafka.KafkaDlqWriter
 * @see com.gotocompany.firehose.sink.dlq.blobstorage.BlobStorageDlqWriter
 * @see com.gotocompany.firehose.sink.dlq.log.LogDlqWriter
 */
public interface DlqWriter {

    /**
     * Method to write messages to dead letter queues destination.
     * @param messages is collection of message that need to be sent to dead letter queue
     * @return collection of messages that failed to be processed
     * @throws IOException can be thrown for non retry able error
     */
    List<Message> write(List<Message> messages) throws IOException;
}
