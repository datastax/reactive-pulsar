package com.github.lhotari.reactive.pulsar.resourceadapter;

import com.github.lhotari.reactive.pulsar.internal.resourceadapter.ResourceWrapperImplementationFactory;
import java.util.function.Supplier;
import org.apache.pulsar.client.api.PulsarClient;

/**
 * ReactivePulsarResourceAdapter provides factories for adapters which are used to handle the lifecycle of
 * PulsarClient {@link org.apache.pulsar.client.api.Producer}, {@link org.apache.pulsar.client.api.Consumer}
 * and {@link org.apache.pulsar.client.api.Reader} instances.
 *
 * This interface is not designed to be used by application code directly. Instead, this is used for the
 * {@link com.github.lhotari.reactive.pulsar.adapter.ReactivePulsarClient} implementations.
 */
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
