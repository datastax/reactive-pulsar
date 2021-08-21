package com.github.lhotari.reactive.pulsar.internal;

import com.github.lhotari.reactive.pulsar.adapter.ReactiveMessageConsumerFactory;
import com.github.lhotari.reactive.pulsar.adapter.ReactiveMessageReaderFactory;
import com.github.lhotari.reactive.pulsar.adapter.ReactiveMessageSenderFactory;
import com.github.lhotari.reactive.pulsar.adapter.ReactivePulsarAdapter;
import com.github.lhotari.reactive.pulsar.adapter.ReactivePulsarClient;
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
        return new DefaultReactiveMessageSenderFactory<>(schema, reactivePulsarAdapter::producer);
    }

    @Override
    public <T> ReactiveMessageConsumerFactory<T> messageConsumer(Schema<T> schema) {
        return new DefaultReactiveMessageConsumerFactory<>(schema, reactivePulsarAdapter.consumer());
    }
}
