package com.github.lhotari.reactive.pulsar.internal.resourceadapter;

import com.github.lhotari.reactive.pulsar.resourceadapter.ReactiveConsumerAdapterFactory;
import com.github.lhotari.reactive.pulsar.resourceadapter.ReactiveProducerAdapterFactory;
import com.github.lhotari.reactive.pulsar.resourceadapter.ReactivePulsarResourceAdapter;
import com.github.lhotari.reactive.pulsar.resourceadapter.ReactiveReaderAdapterFactory;
import java.util.function.Supplier;
import org.apache.pulsar.client.api.PulsarClient;

class DefaultReactivePulsarResourceAdapter implements ReactivePulsarResourceAdapter {

    private final Supplier<PulsarClient> pulsarClientSupplier;

    public DefaultReactivePulsarResourceAdapter(Supplier<PulsarClient> pulsarClientSupplier) {
        this.pulsarClientSupplier = pulsarClientSupplier;
    }

    @Override
    public ReactiveProducerAdapterFactory producer() {
        return new DefaultReactiveProducerAdapterFactory(pulsarClientSupplier);
    }

    @Override
    public ReactiveConsumerAdapterFactory consumer() {
        return new DefaultReactiveConsumerAdapterFactory(pulsarClientSupplier);
    }

    @Override
    public ReactiveReaderAdapterFactory reader() {
        return new DefaultReactiveReaderAdapterFactory(pulsarClientSupplier);
    }
}
