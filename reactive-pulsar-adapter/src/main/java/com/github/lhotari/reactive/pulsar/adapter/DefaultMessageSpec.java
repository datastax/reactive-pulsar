package com.github.lhotari.reactive.pulsar.adapter;

import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import org.apache.pulsar.client.api.TypedMessageBuilder;

class DefaultMessageSpec<T> implements MessageSpec<T> {
    private final String key;
    private final byte[] orderingKey;
    private final byte[] keyBytes;
    private final T value;
    private final Map<String, String> properties;
    private final Long eventTime;
    private final Long sequenceId;
    private final List<String> replicationClusters;
    private final boolean disableReplication;
    private final Long deliverAt;
    private final Long deliverAfterDelay;
    private final TimeUnit deliverAfterUnit;

    DefaultMessageSpec(String key, byte[] orderingKey, byte[] keyBytes, T value, Map<String, String> properties,
                       Long eventTime, Long sequenceId, List<String> replicationClusters,
                       boolean disableReplication,
                       Long deliverAt, Long deliverAfterDelay, TimeUnit deliverAfterUnit) {
        this.key = key;
        this.orderingKey = orderingKey;
        this.keyBytes = keyBytes;
        this.value = value;
        this.properties = properties;
        this.eventTime = eventTime;
        this.sequenceId = sequenceId;
        this.replicationClusters = replicationClusters;
        this.disableReplication = disableReplication;
        this.deliverAt = deliverAt;
        this.deliverAfterDelay = deliverAfterDelay;
        this.deliverAfterUnit = deliverAfterUnit;
    }

    @Override
    public void configure(TypedMessageBuilder<T> typedMessageBuilder) {
        if (key != null) {
            typedMessageBuilder.key(key);
        }
        if (orderingKey != null) {
            typedMessageBuilder.orderingKey(orderingKey);
        }
        if (keyBytes != null) {
            typedMessageBuilder.keyBytes(keyBytes);
        }
        typedMessageBuilder.value(value);
        if (properties != null) {
            typedMessageBuilder.properties(properties);
        }
        if (eventTime != null) {
            typedMessageBuilder.eventTime(eventTime);
        }
        if (sequenceId != null) {
            typedMessageBuilder.sequenceId(sequenceId);
        }
        if (replicationClusters != null) {
            typedMessageBuilder.replicationClusters(replicationClusters);
        }
        if (disableReplication) {
            typedMessageBuilder.disableReplication();
        }
        if (deliverAt != null) {
            typedMessageBuilder.deliverAt(deliverAt);
        }
        if (deliverAfterDelay != null) {
            typedMessageBuilder.deliverAfter(deliverAfterDelay, deliverAfterUnit);
        }
    }
}
