package com.github.lhotari.reactive.pulsar.adapter;

import java.util.Objects;
import org.apache.pulsar.client.api.MessageId;

public final class MessageIdStartAtSpec extends StartAtSpec {

    private final MessageId messageId;
    private final boolean inclusive;

    public MessageIdStartAtSpec(final MessageId messageId, final boolean inclusive) {
        this.messageId = messageId;
        this.inclusive = inclusive;
    }

    public MessageId getMessageId() {
        return this.messageId;
    }

    public boolean isInclusive() {
        return this.inclusive;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        MessageIdStartAtSpec that = (MessageIdStartAtSpec) o;
        return (inclusive == that.inclusive && Objects.equals(messageId, that.messageId));
    }

    @Override
    public int hashCode() {
        return Objects.hash(messageId, inclusive);
    }

    @Override
    public String toString() {
        return ("MessageIdStartAtSpec{" + "messageId=" + messageId + ", inclusive=" + inclusive + '}');
    }
}
