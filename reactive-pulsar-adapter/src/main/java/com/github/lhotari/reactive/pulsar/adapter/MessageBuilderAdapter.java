package com.github.lhotari.reactive.pulsar.adapter;

import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import org.apache.pulsar.client.api.TypedMessageBuilder;

class MessageBuilderAdapter<T> implements MessageBuilder<T> {
    private final TypedMessageBuilder<T> typedMessageBuilder;

    MessageBuilderAdapter(TypedMessageBuilder<T> typedMessageBuilder) {
        this.typedMessageBuilder = typedMessageBuilder;
    }

    @Override
    public MessageBuilder<T> key(String key) {
        typedMessageBuilder.key(key);
        return this;
    }

    @Override
    public MessageBuilder<T> keyBytes(byte[] key) {
        typedMessageBuilder.keyBytes(key);
        return this;
    }

    @Override
    public MessageBuilder<T> orderingKey(byte[] orderingKey) {
        typedMessageBuilder.orderingKey(orderingKey);
        return this;
    }

    @Override
    public MessageBuilder<T> value(T value) {
        typedMessageBuilder.value(value);
        return this;
    }

    @Override
    public MessageBuilder<T> property(String name, String value) {
        typedMessageBuilder.property(name, value);
        return this;
    }

    @Override
    public MessageBuilder<T> properties(Map<String, String> properties) {
        typedMessageBuilder.properties(properties);
        return this;
    }

    @Override
    public MessageBuilder<T> eventTime(long timestamp) {
        typedMessageBuilder.eventTime(timestamp);
        return this;
    }

    @Override
    public MessageBuilder<T> sequenceId(long sequenceId) {
        typedMessageBuilder.sequenceId(sequenceId);
        return this;
    }

    @Override
    public MessageBuilder<T> replicationClusters(List<String> clusters) {
        typedMessageBuilder.replicationClusters(clusters);
        return this;
    }

    @Override
    public MessageBuilder<T> disableReplication() {
        typedMessageBuilder.disableReplication();
        return this;
    }

    @Override
    public MessageBuilder<T> deliverAt(long timestamp) {
        typedMessageBuilder.deliverAt(timestamp);
        return this;
    }

    @Override
    public MessageBuilder<T> deliverAfter(long delay, TimeUnit unit) {
        typedMessageBuilder.deliverAfter(delay, unit);
        return this;
    }
}
