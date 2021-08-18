package com.github.lhotari.reactive.pulsar.adapter;

import org.apache.pulsar.client.api.MessageId;

public interface MessageResult<T> {
    static <T> MessageResult<T> negativeAcknowledge(MessageId messageId) {
        return negativeAcknowledge(messageId, null);
    }

    static <T> MessageResult<T> negativeAcknowledge(MessageId messageId, T value) {
        return new DefaultMessageResult<T>(messageId, false, value);
    }

    static <T> MessageResult<T> acknowledge(MessageId messageId, T value) {
        return new DefaultMessageResult<>(messageId, true, value);
    }

    boolean isAcknowledgeMessage();

    MessageId getMessageId();

    T getValue();
}
