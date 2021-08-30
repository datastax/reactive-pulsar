package com.github.lhotari.reactive.pulsar.internal.resourceadapter;

import com.github.lhotari.reactive.pulsar.internal.adapter.AdapterImplementationFactory;
import com.github.lhotari.reactive.pulsar.resourceadapter.ReactiveReaderAdapter;
import java.util.function.Function;
import java.util.function.Supplier;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.Reader;
import org.apache.pulsar.client.api.ReaderBuilder;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

class DefaultReactiveReaderAdapter<T> implements ReactiveReaderAdapter<T> {

    private final Supplier<PulsarClient> pulsarClientSupplier;
    private final Function<PulsarClient, ReaderBuilder<T>> readerBuilderFactory;

    public DefaultReactiveReaderAdapter(
        Supplier<PulsarClient> pulsarClientSupplier,
        Function<PulsarClient, ReaderBuilder<T>> readerBuilderFactory
    ) {
        this.pulsarClientSupplier = pulsarClientSupplier;
        this.readerBuilderFactory = readerBuilderFactory;
    }

    private Mono<Reader<T>> createReaderMono() {
        return AdapterImplementationFactory.adaptPulsarFuture(() ->
            readerBuilderFactory.apply(pulsarClientSupplier.get()).createAsync()
        );
    }

    private Mono<Void> closeReader(Reader<?> reader) {
        return AdapterImplementationFactory.adaptPulsarFuture(reader::closeAsync);
    }

    @Override
    public <R> Mono<R> usingReader(Function<Reader<T>, Mono<R>> usingReaderAction) {
        return Mono.usingWhen(createReaderMono(), usingReaderAction, this::closeReader);
    }

    @Override
    public <R> Flux<R> usingReaderMany(Function<Reader<T>, Flux<R>> usingReaderAction) {
        return Flux.usingWhen(createReaderMono(), usingReaderAction, this::closeReader);
    }
}
