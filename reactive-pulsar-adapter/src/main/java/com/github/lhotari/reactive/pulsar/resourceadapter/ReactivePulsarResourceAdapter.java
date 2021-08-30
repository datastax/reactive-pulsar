package com.github.lhotari.reactive.pulsar.resourceadapter;

import com.github.lhotari.reactive.pulsar.internal.resourceadapter.ResourceWrapperImplementationFactory;
import java.util.function.Supplier;
import org.apache.pulsar.client.api.PulsarClient;

public interface ReactivePulsarResourceAdapter {
    static ReactivePulsarResourceAdapter create(Supplier<PulsarClient> pulsarClientSupplier) {
        return ResourceWrapperImplementationFactory.createReactivePulsarResourceAdapter(pulsarClientSupplier);
    }

    static ReactivePulsarResourceAdapter create(PulsarClient pulsarClient) {
        return create(() -> pulsarClient);
    }

    ReactiveProducerAdapterFactory producer();

    ReactiveConsumerAdapterFactory consumer();

    ReactiveReaderAdapterFactory reader();
}
