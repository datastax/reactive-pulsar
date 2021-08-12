package com.github.lhotari.reactive.pulsar.adapter;

import java.util.function.Function;
import java.util.function.Supplier;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.ReaderBuilder;

class DefaultReactiveReaderAdapterFactory implements ReactiveReaderAdapterFactory {
    private final Supplier<PulsarClient> pulsarClientSupplier;

    public DefaultReactiveReaderAdapterFactory(Supplier<PulsarClient> pulsarClientSupplier) {
        this.pulsarClientSupplier = pulsarClientSupplier;
    }

    @Override
    public <T> ReactiveReaderAdapter<T> create(Function<PulsarClient, ReaderBuilder<T>> readerBuilderFactory) {
        return new DefaultReactiveReaderAdapter<T>(pulsarClientSupplier, readerBuilderFactory);
    }
}
