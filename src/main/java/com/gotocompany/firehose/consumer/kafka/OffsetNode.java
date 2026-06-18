package com.gotocompany.firehose.consumer.kafka;

import lombok.AllArgsConstructor;
import lombok.Data;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;

/**
 * A single tracked Kafka offset together with its commit-lifecycle flags.
 *
 * <p>Each node pairs a {@link TopicPartition} with the {@link OffsetAndMetadata} to commit and is
 * stored in {@link OffsetManager}. The committable flag marks an offset as safe to commit once its
 * batch has been processed, while the removable flag marks superseded nodes that compaction can
 * discard. Lombok's {@code @Data} generates the getters, setters, {@code equals}, {@code hashCode},
 * and {@code toString}, and {@code @AllArgsConstructor} generates the four-argument constructor.
 */
@Data
@AllArgsConstructor
public class OffsetNode {
    /** Topic and partition this offset belongs to. */
    private TopicPartition topicPartition;
    /** The offset and metadata that would be committed for the partition. */
    private OffsetAndMetadata offsetAndMetadata;
    /** Whether this offset has been released and is safe to commit. */
    private boolean isCommittable;
    /** Whether this offset has been superseded and may be removed during compaction. */
    private boolean isRemovable;

    /**
     * Creates a node that is initially neither committable nor removable.
     *
     * @param topicPartition    the topic and partition for this offset
     * @param offsetAndMetadata the offset and metadata to be committed
     */
    public OffsetNode(TopicPartition topicPartition, OffsetAndMetadata offsetAndMetadata) {
        this(topicPartition, offsetAndMetadata, false, false);
    }
}
