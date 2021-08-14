package com.github.lhotari.reactive.pulsar.adapter;

import static com.github.lhotari.reactive.pulsar.adapter.PulsarFutureAdapter.adaptPulsarFuture;
import java.util.function.Function;
import java.util.function.Supplier;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.ProducerBuilder;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.impl.ProducerBuilderImpl;
import reactor.core.Disposable;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.util.function.Tuple2;
import reactor.util.function.Tuples;

class DefaultReactiveProducerAdapter<T> implements ReactiveProducerAdapter<T> {
    private final ReactiveProducerCache producerCache;
    private final Function<PulsarClient, ProducerBuilder<T>> producerBuilderFactory;
    private final Supplier<PulsarClient> pulsarClientSupplier;
    private Supplier<PublisherTransformer> producerActionTransformer;

    public DefaultReactiveProducerAdapter(ReactiveProducerCache producerCache,
                                          Function<PulsarClient, ProducerBuilder<T>> producerBuilderFactory,
                                          Supplier<PulsarClient> pulsarClientSupplier,
                                          Supplier<PublisherTransformer> producerActionTransformer) {
        this.producerCache = producerCache;
        this.producerBuilderFactory = producerBuilderFactory;
        this.pulsarClientSupplier = pulsarClientSupplier;
        this.producerActionTransformer = producerActionTransformer;
    }

    private Mono<Producer<T>> createProducerMono() {
        return adaptPulsarFuture(() -> producerBuilderFactory.apply(pulsarClientSupplier.get()).createAsync());
    }

    private Mono<Tuple2<DefaultProducerCacheKey, Mono<Producer<T>>>> createCachedProducerKeyAndMono() {
        return Mono.fromCallable(() -> {
            PulsarClient pulsarClient = pulsarClientSupplier.get();
            ProducerBuilderImpl<T> producerBuilder =
                    (ProducerBuilderImpl<T>) producerBuilderFactory.apply(pulsarClient);
            DefaultProducerCacheKey cacheKey = new DefaultProducerCacheKey(pulsarClient,
                    producerBuilder.getConf().clone(), producerBuilder.getSchema());
            return Tuples.of(cacheKey, PulsarFutureAdapter.adaptPulsarFuture(producerBuilder::createAsync));
        });
    }

    private Mono<Void> closeProducer(Producer<?> producer) {
        return adaptPulsarFuture(producer::closeAsync);
    }

    @Override
    public <R> Mono<R> usingProducer(Function<Producer<T>, Mono<R>> usingProducerAction) {
        if (producerCache != null) {
            return usingCachedProducer(usingProducerAction);
        } else {
            return usingUncachedProducer(usingProducerAction);
        }
    }

    private <R> Mono<R> usingUncachedProducer(Function<Producer<T>, Mono<R>> usingProducerAction) {
        return Mono.usingWhen(createProducerMono(),
                producer -> Mono.using(() -> producerActionTransformer.get(),
                        transformer -> usingProducerAction.apply(producer)
                                .as(mono -> Mono.from(transformer.transform(mono))),
                        Disposable::dispose),
                this::closeProducer);
    }

    private <R> Mono<R> usingCachedProducer(Function<Producer<T>, Mono<R>> usingProducerAction) {
        return createCachedProducerKeyAndMono().flatMap(keyAndProducerMono -> {
            ProducerCacheKey cacheKey = keyAndProducerMono.getT1();
            Mono<Producer<T>> producerMono = keyAndProducerMono.getT2();
            return producerCache.usingCachedProducer(cacheKey, producerMono, producerActionTransformer,
                    usingProducerAction);
        });
    }

    @Override
    public <R> Flux<R> usingProducerMany(Function<Producer<T>, Flux<R>> usingProducerAction) {
        if (producerCache != null) {
            return usingCachedProducerMany(usingProducerAction);
        } else {
            return usingUncachedProducerMany(usingProducerAction);
        }
    }

    private <R> Flux<R> usingUncachedProducerMany(Function<Producer<T>, Flux<R>> usingProducerAction) {
        return Flux.usingWhen(createProducerMono(),
                producer -> Flux.using(() -> producerActionTransformer.get(),
                        transformer -> usingProducerAction.apply(producer).as(transformer::transform),
                        Disposable::dispose),
                this::closeProducer);
    }

    private <R> Flux<R> usingCachedProducerMany(Function<Producer<T>, Flux<R>> usingProducerAction) {
        return createCachedProducerKeyAndMono().flatMapMany(keyAndProducerMono -> {
            ProducerCacheKey cacheKey = keyAndProducerMono.getT1();
            Mono<Producer<T>> producerMono = keyAndProducerMono.getT2();
            return producerCache.usingCachedProducerMany(cacheKey, producerMono, producerActionTransformer,
                    usingProducerAction);
        });
    }
}
