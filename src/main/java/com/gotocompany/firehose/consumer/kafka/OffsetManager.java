package com.gotocompany.firehose.consumer.kafka;

import com.gotocompany.firehose.message.Message;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;

import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.TreeSet;
import java.util.stream.Collectors;

/**
 * OffsetManager is a data structure which keeps tracks of all offsets that can be committed to kafka.
 * <p>
 * This class is thread safe. Multiple sinks can use the same object.
 */
public class OffsetManager {
    /** Offsets grouped by batch key; entries become committable when their batch is released. */
    private final Map<Object, Set<OffsetNode>> toBeCommittableBatchOffsets = new HashMap<>();
    /** Per-partition offsets kept sorted by offset for compaction and committable lookups. */
    private final Map<TopicPartition, TreeSet<OffsetNode>> sortedOffsets = new HashMap<>();

    /**
     * @param offsetKeyToMessagesMap A map of key to list of messages to be added
     */
    public synchronized void addOffsetToBatch(Map<Object, List<Message>> offsetKeyToMessagesMap) {
        offsetKeyToMessagesMap.forEach(this::addOffsetToBatch);
    }

    /**
     * Registers the offsets for a list of messages and immediately marks them committable.
     *
     * <p>Used by the synchronous flow, where a batch is fully processed before its offsets are
     * recorded; the messages are tracked under a fixed internal key and released in one step.
     *
     * @param messageList the messages whose offsets are ready to commit
     */
    public synchronized void addOffsetsAndSetCommittable(List<Message> messageList) {
        String syncBatchKey = "sync_batch_key";
        addOffsetToBatch(syncBatchKey, messageList);
        setCommittable(syncBatchKey);
    }

    /**
     * Registers the offsets for every message in the list under the given batch key.
     *
     * @param batch       the key that groups these offsets
     * @param messageList the messages whose offsets to track
     */
    public synchronized void addOffsetToBatch(Object batch, List<Message> messageList) {
        messageList.forEach(m -> addOffsetToBatch(batch, m));
    }

    /**
     * @param batch   key for which this offset belongs to.
     * @param message message to extract offset metadata.
     */
    public synchronized void addOffsetToBatch(Object batch, Message message) {
        OffsetNode currentNode = new OffsetNode(
                new TopicPartition(message.getTopic(), message.getPartition()),
                new OffsetAndMetadata(message.getOffset() + 1));
        addOffsetToBatch(batch, currentNode);
    }

    /**
     * Adds an offset node to both the batch map and the per-partition sorted set.
     *
     * @param batch the key that groups this offset
     * @param node  the offset node to register
     */
    private synchronized void addOffsetToBatch(Object batch, OffsetNode node) {
        toBeCommittableBatchOffsets.computeIfAbsent(batch, x -> new HashSet<>()).add(node);
        sortedOffsets.computeIfAbsent(
                node.getTopicPartition(),
                topicPartition -> new TreeSet<>(Comparator.comparingLong(offsetNode -> offsetNode.getOffsetAndMetadata().offset()))).add(node);
    }

    /**
     * @param batch key for which all offsets can be committed.
     *              Removes the batch from the global map for the cleanup.
     */
    public synchronized void setCommittable(Object batch) {
        toBeCommittableBatchOffsets.getOrDefault(batch, new HashSet<>()).forEach(offsetNode -> offsetNode.setCommittable(true));
        toBeCommittableBatchOffsets.remove(batch);
    }

    /**
     * @return offsets for all partitions
     * It also compact internal sorted list per partition by removing redundant offsets.
     */
    public synchronized Map<TopicPartition, OffsetAndMetadata> getCommittableOffset() {
        return sortedOffsets.entrySet().stream().collect(
                Collectors.toMap(
                        Map.Entry::getKey,
                        kv -> compactAndFetchFirstCommittableNode(kv.getValue())
                )).entrySet().stream().filter(kv -> kv.getValue().isPresent()).collect(
                Collectors.toMap(Map.Entry::getKey, entry -> entry.getValue().get().getOffsetAndMetadata()));
    }

    /**
     * @param nodes Sorted List of offsets
     * @return the first offset that is set to be committable just before a non-committable offset in the list.
     */
    protected Optional<OffsetNode> compactAndFetchFirstCommittableNode(TreeSet<OffsetNode> nodes) {
        if (nodes.size() == 0) {
            return Optional.empty();
        }
        Iterator<OffsetNode> iterator = nodes.iterator();
        OffsetNode current = null;
        OffsetNode previous;
        while (iterator.hasNext()) {
            previous = current;
            current = iterator.next();
            if (!current.isCommittable()) {
                break;
            }
            if (previous != null) {
                previous.setRemovable(true);
            }
        }

        // Compact
        iterator = nodes.iterator();
        while (iterator.hasNext()) {
            if (iterator.next().isRemovable()) {
                iterator.remove();
            } else {
                break;
            }
        }
        return nodes.first().isCommittable() ? Optional.of(nodes.first()) : Optional.empty();
    }

    /**
     * Returns the sorted offset set currently tracked for the given partition.
     *
     * @param topicPartition the partition to look up
     * @return the partition's sorted offsets, or {@code null} if none are tracked
     */
    protected TreeSet<OffsetNode> getOffsetsForTopicPartition(TopicPartition topicPartition) {
        return sortedOffsets.get(topicPartition);
    }

    /**
     * Returns the offsets registered under the given batch key.
     *
     * @param key the batch key to look up
     * @return the offsets registered for the batch, or {@code null} if the batch is unknown
     */
    protected Set<OffsetNode> getOffsetsForBatch(Object key) {
        return toBeCommittableBatchOffsets.get(key);
    }


}
