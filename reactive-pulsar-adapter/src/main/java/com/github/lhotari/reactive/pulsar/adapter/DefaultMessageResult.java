package com.github.lhotari.reactive.pulsar.adapter;

import org.apache.pulsar.client.api.MessageId;

class DefaultMessageResult<T> implements MessageResult<T> {
    private final MessageId messageId;
    private final boolean acknowledgeMessage;
    private final T value;

    DefaultMessageResult(MessageId messageId, boolean acknowledgeMessage, T value) {
        this.messageId = messageId;
        this.acknowledgeMessage = acknowledgeMessage;
        this.value = value;
    }

    @Override
    public MessageId getMessageId() {
        return messageId;
    }

    @Override
    public boolean isAcknowledgeMessage() {
        return acknowledgeMessage;
    }

    @Override
    public T getValue() {
        return value;
    }
}
