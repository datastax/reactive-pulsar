package com.github.lhotari.reactive.pulsar.internal;

import com.github.lhotari.reactive.pulsar.adapter.MessageSpec;
import com.github.lhotari.reactive.pulsar.adapter.MessageSpecBuilder;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

class DefaultMessageSpecBuilder<T> implements MessageSpecBuilder<T> {
    private String key;
    private byte[] orderingKey;
    private byte[] keyBytes;
    private T value;
    private Map<String, String> properties;
    private Long eventTime;
    private Long sequenceId;
    private List<String> replicationClusters;
    private boolean disableReplication;
    private Long deliverAt;
    private Long deliverAfterDelay;
    private TimeUnit deliverAfterUnit;

    @Override
    public MessageSpecBuilder<T> key(String key) {
        this.key = key;
        return this;
    }

    @Override
    public MessageSpecBuilder<T> keyBytes(byte[] key) {
        this.keyBytes = key;
        return this;
    }

    @Override
    public MessageSpecBuilder<T> orderingKey(byte[] orderingKey) {
        this.orderingKey = orderingKey;
        return this;
    }

    @Override
    public MessageSpecBuilder<T> value(T value) {
        this.value = value;
        return this;
    }

    @Override
    public MessageSpecBuilder<T> property(String name, String value) {
        if (this.properties == null) {
            this.properties = new HashMap<>();
        }
        this.properties.put(name, value);
        return this;
    }

    @Override
    public MessageSpecBuilder<T> properties(Map<String, String> properties) {
        if (this.properties == null) {
            this.properties = new HashMap<>();
        }
        this.properties.putAll(properties);
        return this;
    }

    @Override
    public MessageSpecBuilder<T> eventTime(long timestamp) {
        this.eventTime = timestamp;
        return this;
    }

    @Override
    public MessageSpecBuilder<T> sequenceId(long sequenceId) {
        this.sequenceId = sequenceId;
        return this;
    }

    @Override
    public MessageSpecBuilder<T> replicationClusters(List<String> clusters) {
        this.replicationClusters = new ArrayList<>(clusters);
        return this;
    }

    @Override
    public MessageSpecBuilder<T> disableReplication() {
        this.disableReplication = true;
        return this;
    }

    @Override
    public MessageSpecBuilder<T> deliverAt(long timestamp) {
        this.deliverAt = timestamp;
        return this;
    }

    @Override
    public MessageSpecBuilder<T> deliverAfter(long delay, TimeUnit unit) {
        this.deliverAfterDelay = delay;
        this.deliverAfterUnit = unit;
        return this;
    }

    @Override
    public MessageSpec<T> build() {
        return new DefaultMessageSpec<T>(key, orderingKey, keyBytes, value, properties, eventTime, sequenceId,
                replicationClusters, disableReplication, deliverAt, deliverAfterDelay, deliverAfterUnit);
    }
}
