package com.github.lhotari.reactive.pulsar.adapter;

import java.time.Instant;
import org.apache.pulsar.client.api.MessageId;

public abstract class StartAtSpec {

    private static final MessageIdStartAtSpec EARLIEST = ofMessageId(MessageId.earliest, true);
    private static final MessageIdStartAtSpec LATEST = ofMessageId(MessageId.latest, false);
    private static final MessageIdStartAtSpec LATEST_INCLUSIVE = ofMessageId(MessageId.latest, true);

    StartAtSpec() {}

    public static MessageIdStartAtSpec ofEarliest() {
        return EARLIEST;
    }

    public static MessageIdStartAtSpec ofLatest() {
        return LATEST;
    }

    public static MessageIdStartAtSpec ofLatestInclusive() {
        return LATEST_INCLUSIVE;
    }

    public static MessageIdStartAtSpec ofMessageId(MessageId messageId, boolean inclusive) {
        return new MessageIdStartAtSpec(messageId, inclusive);
    }

    public static MessageIdStartAtSpec ofMessageId(MessageId messageId) {
        return ofMessageId(messageId, false);
    }

    public static InstantStartAtSpec ofInstant(Instant instant) {
        return new InstantStartAtSpec(instant);
    }
}
