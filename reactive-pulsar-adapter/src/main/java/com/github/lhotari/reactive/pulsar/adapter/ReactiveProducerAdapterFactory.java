package com.github.lhotari.reactive.pulsar.adapter;

import java.util.function.Function;
import java.util.function.Supplier;
import org.apache.pulsar.client.api.ProducerBuilder;
import org.apache.pulsar.client.api.PulsarClient;

public interface ReactiveProducerAdapterFactory {
    ReactiveProducerAdapterFactory cache(ReactiveProducerCache producerCache);

    ReactiveProducerAdapterFactory producerActionTransformer(Supplier<PublisherTransformer> producerActionTransformer);

    <T> ReactiveProducerAdapter<T> create(Function<PulsarClient, ProducerBuilder<T>> producerBuilderFactory);
}
