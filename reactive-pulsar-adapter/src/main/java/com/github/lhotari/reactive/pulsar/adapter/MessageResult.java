package com.github.lhotari.reactive.pulsar.adapter;

import org.apache.pulsar.client.api.MessageId;

public interface MessageResult<T> {
    static <T> MessageResult<T> acknowledge(MessageId messageId, T value) {
        return new DefaultMessageResult<>(messageId, true, value);
    }

    static <T> MessageResult<T> negativeAcknowledge(MessageId messageId, T value) {
        return new DefaultMessageResult<T>(messageId, false, value);
    }

    static MessageResult<Void> acknowledge(MessageId messageId) {
        return new EmptyMessageResult(messageId, true);
    }

    static MessageResult<Void> negativeAcknowledge(MessageId messageId) {
        return new EmptyMessageResult(messageId, false);
    }

    boolean isAcknowledgeMessage();

    MessageId getMessageId();

    T getValue();
}
