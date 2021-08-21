package com.github.lhotari.reactive.pulsar.adapter;

import com.github.lhotari.reactive.pulsar.internal.DefaultImplementationFactory;
import java.util.function.Supplier;
import org.apache.pulsar.client.api.PulsarClient;

public interface ReactivePulsarAdapter {
    static ReactivePulsarAdapter create(Supplier<PulsarClient> pulsarClientSupplier) {
        return DefaultImplementationFactory.createReactivePulsarAdapter(pulsarClientSupplier);
    }

    static ReactivePulsarAdapter create(PulsarClient pulsarClient) {
        return create(() -> pulsarClient);
    }

    ReactiveProducerAdapterFactory producer();

    ReactiveConsumerAdapterFactory consumer();

    ReactiveReaderAdapterFactory reader();
}
