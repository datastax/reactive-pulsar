package com.github.lhotari.reactive.pulsar.internal;

import com.github.lhotari.reactive.pulsar.adapter.MessageResult;
import org.apache.pulsar.client.api.MessageId;

class EmptyMessageResult implements MessageResult<Void> {
    private final MessageId messageId;
    private final boolean acknowledgeMessage;

    EmptyMessageResult(MessageId messageId, boolean acknowledgeMessage) {
        this.messageId = messageId;
        this.acknowledgeMessage = acknowledgeMessage;
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
    public Void getValue() {
        return null;
    }
}
