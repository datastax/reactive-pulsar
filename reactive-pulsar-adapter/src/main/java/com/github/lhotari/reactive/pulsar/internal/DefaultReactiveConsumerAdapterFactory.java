package com.github.lhotari.reactive.pulsar.internal;

import com.github.lhotari.reactive.pulsar.adapter.ReactiveConsumerAdapter;
import com.github.lhotari.reactive.pulsar.adapter.ReactiveConsumerAdapterFactory;
import java.util.function.Function;
import java.util.function.Supplier;
import org.apache.pulsar.client.api.ConsumerBuilder;
import org.apache.pulsar.client.api.PulsarClient;

class DefaultReactiveConsumerAdapterFactory implements ReactiveConsumerAdapterFactory {

    private final Supplier<PulsarClient> pulsarClientSupplier;

    DefaultReactiveConsumerAdapterFactory(Supplier<PulsarClient> pulsarClientSupplier) {
        this.pulsarClientSupplier = pulsarClientSupplier;
    }

    @Override
    public <T> ReactiveConsumerAdapter<T> create(Function<PulsarClient, ConsumerBuilder<T>> consumerBuilderFactory) {
        return new DefaultReactiveConsumerAdapter<T>(pulsarClientSupplier, consumerBuilderFactory);
    }
}
