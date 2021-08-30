package com.github.lhotari.reactive.pulsar.resourceadapter;

import java.util.function.Function;
import org.apache.pulsar.client.api.Consumer;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

public interface ReactiveConsumerAdapter<T> {
    <R> Mono<R> usingConsumer(Function<Consumer<T>, Mono<R>> usingConsumerAction);

    <R> Flux<R> usingConsumerMany(Function<Consumer<T>, Flux<R>> usingConsumerAction);
}
