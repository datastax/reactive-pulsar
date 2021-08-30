package com.github.lhotari.reactive.pulsar.internal;

import com.github.lhotari.reactive.pulsar.adapter.*;
import org.apache.pulsar.client.api.Schema;

class DefaultReactivePulsarClient implements ReactivePulsarClient {

    private final ReactivePulsarAdapter reactivePulsarAdapter;

    DefaultReactivePulsarClient(ReactivePulsarAdapter reactivePulsarAdapter) {
        this.reactivePulsarAdapter = reactivePulsarAdapter;
    }

    @Override
    public <T> ReactiveMessageReaderBuilder<T> messageReader(Schema<T> schema) {
        return new DefaultReactiveMessageReaderBuilder<>(schema, reactivePulsarAdapter.reader());
    }

    @Override
    public <T> ReactiveMessageSenderBuilder<T> messageSender(Schema<T> schema) {
        return new DefaultReactiveMessageSenderBuilder<>(schema, reactivePulsarAdapter.producer());
    }

    @Override
    public <T> ReactiveMessageConsumerBuilder<T> messageConsumer(Schema<T> schema) {
        return new DefaultReactiveMessageConsumerBuilder<>(schema, reactivePulsarAdapter.consumer());
    }
}
