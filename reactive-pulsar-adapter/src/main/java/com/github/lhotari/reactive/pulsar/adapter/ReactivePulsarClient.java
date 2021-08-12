package com.github.lhotari.reactive.pulsar.adapter;

import java.util.function.Supplier;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.Schema;

public interface ReactivePulsarClient {
    static ReactivePulsarClient create(PulsarClient pulsarClient) {
        return create(() -> pulsarClient);
    }

    static ReactivePulsarClient create(Supplier<PulsarClient> pulsarClientSupplier) {
        return create(ReactivePulsarAdapter.create(pulsarClientSupplier));
    }

    static ReactivePulsarClient create(ReactivePulsarAdapter reactivePulsarAdapter) {
        return new DefaultReactivePulsarClient(reactivePulsarAdapter);
    }

    <T> ReactiveMessageReaderFactory<T> messageReader(Schema<T> schema);
    <T> ReactiveMessageSenderFactory<T> messageSender(Schema<T> schema);
    <T> ReactiveMessageHandlerBuilder<T> messageHandler(Schema<T> schema);
}
