package com.github.lhotari.reactive.pulsar.adapter;

import java.util.function.Function;
import org.apache.pulsar.client.api.Producer;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

public interface ReactiveProducerCache {
    <T, R> Mono<R> usingCachedProducer(ProducerCacheKey cacheKey, Mono<Producer<T>> producerMono, Function<Producer<T>, Mono<R>> usingProducerAction);

    <T, R> Flux<R> usingCachedProducerMany(ProducerCacheKey cacheKey, Mono<Producer<T>> producerMono, Function<Producer<T>, Flux<R>> usingProducerManyAction);
}
