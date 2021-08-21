package com.github.lhotari.reactive.pulsar.adapter;

import com.github.lhotari.reactive.pulsar.internal.DefaultImplementationFactory;
import org.apache.pulsar.client.api.MessageId;

public interface MessageResult<T> {
    static <T> MessageResult<T> acknowledge(MessageId messageId, T value) {
        return DefaultImplementationFactory.acknowledge(messageId, value);
    }

    static <T> MessageResult<T> negativeAcknowledge(MessageId messageId, T value) {
        return DefaultImplementationFactory.negativeAcknowledge(messageId, value);
    }

    static MessageResult<Void> acknowledge(MessageId messageId) {
        return DefaultImplementationFactory.acknowledge(messageId);
    }

    static MessageResult<Void> negativeAcknowledge(MessageId messageId) {
        return DefaultImplementationFactory.negativeAcknowledge(messageId);
    }

    boolean isAcknowledgeMessage();

    MessageId getMessageId();

    T getValue();
}
