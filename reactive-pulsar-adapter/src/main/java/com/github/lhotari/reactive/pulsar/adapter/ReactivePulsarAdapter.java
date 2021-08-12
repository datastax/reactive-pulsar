package com.github.lhotari.reactive.pulsar.adapter;

import java.util.function.Supplier;
import org.apache.pulsar.client.api.PulsarClient;

public interface ReactivePulsarAdapter {
    static ReactivePulsarAdapter create(Supplier<PulsarClient> pulsarClientSupplier) {
        return new DefaultReactivePulsarAdapter(pulsarClientSupplier);
    }

    static ReactivePulsarAdapter create(PulsarClient pulsarClient) {
        return create(() -> pulsarClient);
    }

    ReactiveProducerAdapterFactory producer();

    ReactiveConsumerAdapterFactory consumer();

    ReactiveReaderAdapterFactory reader();
}
