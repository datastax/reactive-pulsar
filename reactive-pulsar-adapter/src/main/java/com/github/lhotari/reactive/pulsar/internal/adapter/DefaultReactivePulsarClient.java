package com.github.lhotari.reactive.pulsar.internal.adapter;

import com.github.lhotari.reactive.pulsar.adapter.ReactiveMessageConsumerBuilder;
import com.github.lhotari.reactive.pulsar.adapter.ReactiveMessageReaderBuilder;
import com.github.lhotari.reactive.pulsar.adapter.ReactiveMessageSenderBuilder;
import com.github.lhotari.reactive.pulsar.adapter.ReactivePulsarClient;
import com.github.lhotari.reactive.pulsar.resourceadapter.ReactivePulsarResourceAdapter;
import org.apache.pulsar.client.api.Schema;

class DefaultReactivePulsarClient implements ReactivePulsarClient {

    private final ReactivePulsarResourceAdapter reactivePulsarResourceAdapter;

    DefaultReactivePulsarClient(ReactivePulsarResourceAdapter reactivePulsarResourceAdapter) {
        this.reactivePulsarResourceAdapter = reactivePulsarResourceAdapter;
    }

    @Override
    public <T> ReactiveMessageReaderBuilder<T> messageReader(Schema<T> schema) {
        return new DefaultReactiveMessageReaderBuilder<>(schema, reactivePulsarResourceAdapter.reader());
    }

    @Override
    public <T> ReactiveMessageSenderBuilder<T> messageSender(Schema<T> schema) {
        return new DefaultReactiveMessageSenderBuilder<>(schema, reactivePulsarResourceAdapter.producer());
    }

    @Override
    public <T> ReactiveMessageConsumerBuilder<T> messageConsumer(Schema<T> schema) {
        return new DefaultReactiveMessageConsumerBuilder<>(schema, reactivePulsarResourceAdapter.consumer());
    }
}
