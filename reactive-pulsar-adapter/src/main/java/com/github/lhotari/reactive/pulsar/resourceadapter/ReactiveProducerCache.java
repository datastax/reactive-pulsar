package com.github.lhotari.reactive.pulsar.resourceadapter;

import java.util.function.Function;
import java.util.function.Supplier;
import org.apache.pulsar.client.api.Producer;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

/**
 * Producer cache implementations must implement this interface. There's a sample implementation in the
 * reactive-pulsar-caffeine-producer-cache which uses Caffeine.
 * This interface is not intended to be used by application code directly.
 */
public interface ReactiveProducerCache {
    <T, R> Mono<R> usingCachedProducer(
        ProducerCacheKey cacheKey,
        Mono<Producer<T>> producerMono,
        Supplier<PublisherTransformer> producerActionTransformer,
        Function<Producer<T>, Mono<R>> usingProducerAction
    );

    <T, R> Flux<R> usingCachedProducerMany(
        ProducerCacheKey cacheKey,
        Mono<Producer<T>> producerMono,
        Supplier<PublisherTransformer> producerActionTransformer,
        Function<Producer<T>, Flux<R>> usingProducerManyAction
    );
}
