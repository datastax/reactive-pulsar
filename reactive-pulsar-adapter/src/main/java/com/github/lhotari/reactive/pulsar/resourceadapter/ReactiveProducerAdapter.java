package com.github.lhotari.reactive.pulsar.resourceadapter;

import java.util.function.Function;
import org.apache.pulsar.client.api.Producer;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

public interface ReactiveProducerAdapter<T> {
    <R> Mono<R> usingProducer(Function<Producer<T>, Mono<R>> usingProducerAction);
    <R> Flux<R> usingProducerMany(Function<Producer<T>, Flux<R>> usingProducerAction);
}
