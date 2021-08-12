package com.github.lhotari.reactive.pulsar.adapter;

import org.apache.pulsar.client.api.Schema;

class DefaultReactivePulsarClient implements ReactivePulsarClient {
    private final ReactivePulsarAdapter reactivePulsarAdapter;

    DefaultReactivePulsarClient(ReactivePulsarAdapter reactivePulsarAdapter) {
        this.reactivePulsarAdapter = reactivePulsarAdapter;
    }

    @Override
    public <T> ReactiveMessageReaderFactory<T> messageReader(Schema<T> schema) {
        return new DefaultReactiveMessageReaderFactory<>(schema, reactivePulsarAdapter.reader());
    }

    @Override
    public <T> ReactiveMessageSenderFactory<T> messageSender(Schema<T> schema) {
        return new DefaultReactiveMessageSenderFactory<>(schema, reactivePulsarAdapter.producer());
    }

    @Override
    public <T> ReactiveMessageHandlerBuilder<T> messageHandler(Schema<T> schema) {
        return new DefaultReactiveMessageHandlerBuilder<>(schema, reactivePulsarAdapter.consumer());
    }

    @Override
    public <T> ReactiveMessageConsumerFactory<T> messageConsumer(Schema<T> schema) {
        return new DefaultReactiveMessageConsumerFactory<>(schema, reactivePulsarAdapter.consumer());
    }


}
