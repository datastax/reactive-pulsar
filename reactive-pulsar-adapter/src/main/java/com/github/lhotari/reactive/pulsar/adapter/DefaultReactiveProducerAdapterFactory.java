package com.github.lhotari.reactive.pulsar.adapter;

import java.util.function.Function;
import java.util.function.Supplier;
import org.apache.pulsar.client.api.ProducerBuilder;
import org.apache.pulsar.client.api.PulsarClient;

class DefaultReactiveProducerAdapterFactory implements ReactiveProducerAdapterFactory {
    private final Supplier<PulsarClient> pulsarClientSupplier;
    private ReactiveProducerCache producerCache;
    private Supplier<PublisherTransformer> producerActionTransformer = PublisherTransformer::identity;

    public DefaultReactiveProducerAdapterFactory(Supplier<PulsarClient> pulsarClientSupplier) {
        this.pulsarClientSupplier = pulsarClientSupplier;
    }

    public ReactiveProducerAdapterFactory cache(ReactiveProducerCache producerCache) {
        this.producerCache = producerCache;
        return this;
    }

    public ReactiveProducerAdapterFactory producerActionTransformer(
            Supplier<PublisherTransformer> producerActionTransformer) {
        this.producerActionTransformer = producerActionTransformer;
        return this;
    }

    @Override
    public <T> ReactiveProducerAdapter<T> create(Function<PulsarClient, ProducerBuilder<T>> producerBuilderFactory) {
        return new DefaultReactiveProducerAdapter<T>(producerCache, producerBuilderFactory, pulsarClientSupplier,
                producerActionTransformer);
    }
}
