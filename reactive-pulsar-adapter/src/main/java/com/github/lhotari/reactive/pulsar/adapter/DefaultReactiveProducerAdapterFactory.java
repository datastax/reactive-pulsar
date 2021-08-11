package com.github.lhotari.reactive.pulsar.adapter;

import java.util.function.Function;
import java.util.function.Supplier;
import org.apache.pulsar.client.api.ProducerBuilder;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.Schema;

class DefaultReactiveProducerAdapterFactory
        implements ReactiveProducerAdapterFactory {
    private final Supplier<PulsarClient> pulsarClientSupplier;
    ReactiveProducerCache producerCache;

    public DefaultReactiveProducerAdapterFactory(Supplier<PulsarClient> pulsarClientSupplier) {
        this.pulsarClientSupplier = pulsarClientSupplier;
    }

    @Override
    public ReactiveProducerAdapterFactory cache(ReactiveProducerCache producerCache) {
        this.producerCache = producerCache;
        return this;
    }

    @Override
    public <T> ReactiveProducerAdapter<T> create(Function<PulsarClient, ProducerBuilder<T>> producerBuilderFactory) {
        return new DefaultReactiveProducerAdapter<T>(producerCache, producerBuilderFactory, pulsarClientSupplier);
    }

    @Override
    public <T> ReactiveMessageSenderFactory<T> messageSender(Schema<T> schema) {
        return new DefaultReactiveMessageSenderFactory<T>(schema, this);
    }
}
