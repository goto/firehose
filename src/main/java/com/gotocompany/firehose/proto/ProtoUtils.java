package com.gotocompany.firehose.proto;

import com.google.protobuf.DynamicMessage;

import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.Queue;
import java.util.stream.Collectors;

/**
 * Static helpers for inspecting decoded protobuf messages.
 *
 * <p>Provides detection of unknown (unmapped) fields, which Firehose uses to decide whether a
 * message was parsed against an outdated schema and should be retried or routed to the DLQ.
 */
public class ProtoUtils {
    /**
     * Returns whether the message or any nested message contains unknown fields.
     *
     * @param root the decoded protobuf message to inspect
     * @return {@code true} if any, possibly nested, message has unknown fields
     */
    public static boolean hasUnknownField(DynamicMessage root) {
        List<DynamicMessage> dynamicMessageFields = collectNestedFields(root);
        List<DynamicMessage> messageWithUnknownFields = getMessageWithUnknownFields(dynamicMessageFields);
        return messageWithUnknownFields.size() > 0;
    }

    /**
     * Collects the given message and all of its nested message fields via an iterative traversal.
     *
     * @param node the root message to traverse
     * @return a list containing the root and every nested message
     */
    private static List<DynamicMessage> collectNestedFields(DynamicMessage node) {
        List<DynamicMessage> output = new LinkedList<>();
        Queue<DynamicMessage> stack = Collections.asLifoQueue(new LinkedList<>());
        stack.add(node);
        while (true) {
            DynamicMessage current = stack.poll();
            if (current == null) {
                break;
            }
            List<DynamicMessage> nestedChildNodes = current.getAllFields().values().stream()
                    .filter(field -> field instanceof DynamicMessage)
                    .map(field -> (DynamicMessage) field)
                    .collect(Collectors.toList());
            stack.addAll(nestedChildNodes);

            output.add(current);
        }

        return output;
    }

    /**
     * Filters the given messages down to those that carry unknown fields.
     *
     * @param messages the messages to inspect
     * @return the subset of messages that have unknown fields
     */
    private static List<DynamicMessage> getMessageWithUnknownFields(List<DynamicMessage> messages) {
        return messages.stream().filter(message -> message.getUnknownFields().asMap().size() > 0).collect(Collectors.toList());
    }
}
