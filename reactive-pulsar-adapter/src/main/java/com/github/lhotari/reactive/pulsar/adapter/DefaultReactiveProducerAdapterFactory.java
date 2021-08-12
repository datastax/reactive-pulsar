package com.github.lhotari.reactive.pulsar.adapter;

import java.util.function.Function;
import java.util.function.Supplier;
import org.apache.pulsar.client.api.ProducerBuilder;
import org.apache.pulsar.client.api.PulsarClient;

class DefaultReactiveProducerAdapterFactory
        implements ReactiveProducerAdapterFactory {
    private final Supplier<PulsarClient> pulsarClientSupplier;

    public DefaultReactiveProducerAdapterFactory(Supplier<PulsarClient> pulsarClientSupplier) {
        this.pulsarClientSupplier = pulsarClientSupplier;
    }

    @Override
    public <T> ReactiveProducerAdapter<T> create(Function<PulsarClient, ProducerBuilder<T>> producerBuilderFactory,
                                                 ReactiveProducerCache producerCache) {
        return new DefaultReactiveProducerAdapter<T>(producerCache, producerBuilderFactory, pulsarClientSupplier);
    }
}
