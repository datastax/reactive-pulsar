package com.github.lhotari.reactive.pulsar.internal.resourcewrapper;

import com.github.lhotari.reactive.pulsar.resourcewrapper.PublisherTransformer;
import com.github.lhotari.reactive.pulsar.resourcewrapper.ReactiveProducerAdapter;
import com.github.lhotari.reactive.pulsar.resourcewrapper.ReactiveProducerAdapterFactory;
import com.github.lhotari.reactive.pulsar.resourcewrapper.ReactiveProducerCache;
import java.util.function.Function;
import java.util.function.Supplier;
import org.apache.pulsar.client.api.ProducerBuilder;
import org.apache.pulsar.client.api.PulsarClient;

class DefaultReactiveProducerAdapterFactory implements ReactiveProducerAdapterFactory {

    private final Supplier<PulsarClient> pulsarClientSupplier;

    public DefaultReactiveProducerAdapterFactory(Supplier<PulsarClient> pulsarClientSupplier) {
        this.pulsarClientSupplier = pulsarClientSupplier;
    }

    @Override
    public <T> ReactiveProducerAdapter<T> create(
        Function<PulsarClient, ProducerBuilder<T>> producerBuilderFactory,
        ReactiveProducerCache producerCache,
        Supplier<PublisherTransformer> producerActionTransformer
    ) {
        return new DefaultReactiveProducerAdapter<>(
            pulsarClientSupplier,
            producerBuilderFactory,
            producerCache,
            producerActionTransformer == null ? PublisherTransformer::identity : producerActionTransformer
        );
    }
}
