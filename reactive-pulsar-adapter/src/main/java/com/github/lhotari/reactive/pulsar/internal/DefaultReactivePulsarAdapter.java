package com.github.lhotari.reactive.pulsar.internal;

import com.github.lhotari.reactive.pulsar.adapter.ReactiveConsumerAdapterFactory;
import com.github.lhotari.reactive.pulsar.adapter.ReactiveProducerAdapterFactory;
import com.github.lhotari.reactive.pulsar.adapter.ReactivePulsarAdapter;
import com.github.lhotari.reactive.pulsar.adapter.ReactiveReaderAdapterFactory;
import java.util.function.Supplier;
import org.apache.pulsar.client.api.PulsarClient;

class DefaultReactivePulsarAdapter implements ReactivePulsarAdapter {
    private final Supplier<PulsarClient> pulsarClientSupplier;

    public DefaultReactivePulsarAdapter(Supplier<PulsarClient> pulsarClientSupplier) {
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