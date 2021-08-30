package com.github.lhotari.reactive.pulsar.adapter;

import com.github.lhotari.reactive.pulsar.internal.adapter.AdapterImplementationFactory;
import org.apache.pulsar.client.api.MessageId;

public interface MessageResult<T> {
    static <T> MessageResult<T> acknowledge(MessageId messageId, T value) {
        return AdapterImplementationFactory.acknowledge(messageId, value);
    }

    static <T> MessageResult<T> negativeAcknowledge(MessageId messageId, T value) {
        return AdapterImplementationFactory.negativeAcknowledge(messageId, value);
    }

    static MessageResult<Void> acknowledge(MessageId messageId) {
        return AdapterImplementationFactory.acknowledge(messageId);
    }

    static MessageResult<Void> negativeAcknowledge(MessageId messageId) {
        return AdapterImplementationFactory.negativeAcknowledge(messageId);
    }

    boolean isAcknowledgeMessage();

    MessageId getMessageId();

    T getValue();
}
