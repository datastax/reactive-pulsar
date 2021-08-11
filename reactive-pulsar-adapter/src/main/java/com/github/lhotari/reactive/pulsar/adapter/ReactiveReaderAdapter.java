package com.github.lhotari.reactive.pulsar.adapter;

import java.util.function.Function;
import org.apache.pulsar.client.api.Reader;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

public interface ReactiveReaderAdapter<T> {
    <R> Mono<R> usingReader(Function<Reader<T>, Mono<R>> usingReaderAction);

    <R> Flux<R> usingReaderMany(Function<Reader<T>, Flux<R>> usingReaderAction);
}
