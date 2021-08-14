package com.github.lhotari.reactive.pulsar.adapter;

import java.util.function.Function;
import java.util.function.Supplier;
import org.apache.pulsar.client.api.Producer;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

public interface ReactiveProducerCache {
    <T, R> Mono<R> usingCachedProducer(ProducerCacheKey cacheKey, Mono<Producer<T>> producerMono,
                                       Supplier<PublisherTransformer> producerActionTransformer,
                                       Function<Producer<T>, Mono<R>> usingProducerAction);

    <T, R> Flux<R> usingCachedProducerMany(ProducerCacheKey cacheKey, Mono<Producer<T>> producerMono,
                                           Supplier<PublisherTransformer> producerActionTransformer,
                                           Function<Producer<T>, Flux<R>> usingProducerManyAction);
}
