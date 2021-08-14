package com.github.lhotari.reactive.pulsar.producercache;

import com.github.benmanes.caffeine.cache.AsyncCache;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.CaffeineSpec;
import com.github.benmanes.caffeine.cache.RemovalCause;
import com.github.benmanes.caffeine.cache.Scheduler;
import com.github.lhotari.reactive.pulsar.adapter.ProducerCacheKey;
import com.github.lhotari.reactive.pulsar.adapter.PublisherTransformer;
import com.github.lhotari.reactive.pulsar.adapter.PulsarFutureAdapter;
import com.github.lhotari.reactive.pulsar.adapter.ReactiveProducerCache;
import java.time.Duration;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import java.util.function.Supplier;
import org.apache.pulsar.client.api.Producer;
import org.reactivestreams.Publisher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;

public class CaffeineReactiveProducerCache implements ReactiveProducerCache, AutoCloseable {
    private static final Logger log = LoggerFactory.getLogger(CaffeineReactiveProducerCache.class);
    final AsyncCache<ProducerCacheKey, CaffeineReactiveProducerCache.ProducerCacheEntry> cache;

    public CaffeineReactiveProducerCache() {
        this(Caffeine.newBuilder()
                .expireAfterAccess(Duration.ofMinutes(1))
                .expireAfterWrite(Duration.ofMinutes(10))
                .maximumSize(1000));
    }

    public CaffeineReactiveProducerCache(CaffeineSpec caffeineSpec) {
        this(Caffeine.from(caffeineSpec));
    }

    public CaffeineReactiveProducerCache(Caffeine<Object, Object> caffeineBuilder) {
        this.cache = caffeineBuilder
                .scheduler(Scheduler.systemScheduler())
                .executor(Schedulers.boundedElastic()::schedule)
                .removalListener(this::onRemoval)
                .buildAsync();
    }

    private void onRemoval(ProducerCacheKey key, CaffeineReactiveProducerCache.ProducerCacheEntry entry,
                           RemovalCause cause) {
        entry.close();
    }

    private static void flushAndCloseProducerAsync(Producer<?> producer) {
        producer.flushAsync()
                .thenCompose((__) -> producer.closeAsync())
                .whenComplete((r, t) -> {
                    if (t != null) {
                        log.error("Error flushing and closing producer", t);
                    }
                });
    }

    private <T> Mono<CaffeineReactiveProducerCache.ProducerCacheEntry> getProducerCacheEntry(
            ProducerCacheKey cacheKey,
            Mono<Producer<T>> producerMono,
            Supplier<PublisherTransformer> producerActionTransformer) {
        return PulsarFutureAdapter.adaptPulsarFuture(() ->
                        this.cache.get(cacheKey,
                                (__, ___) -> producerMono.<ProducerCacheEntry>map(
                                                producer -> new ProducerCacheEntry(producer,
                                                        producerActionTransformer != null ?
                                                                producerActionTransformer
                                                                : null))
                                        .toFuture()))
                .flatMap(producerCacheEntry -> producerCacheEntry.recreateIfClosed(producerMono));
    }

    public void close() {
        this.cache.synchronous().invalidateAll();
    }

    @Override
    public <T, R> Mono<R> usingCachedProducer(ProducerCacheKey cacheKey, Mono<Producer<T>> producerMono,
                                              Supplier<PublisherTransformer> producerActionTransformer,
                                              Function<Producer<T>, Mono<R>> usingProducerAction) {
        return Mono.usingWhen(this.leaseCacheEntry(cacheKey, producerMono, producerActionTransformer),
                producerCacheEntry -> usingProducerAction.apply(producerCacheEntry.getProducer())
                        .as(producerCacheEntry::decorateProducerAction),
                producerCacheEntry -> this.returnCacheEntry(producerCacheEntry));
    }

    private Mono<Object> returnCacheEntry(CaffeineReactiveProducerCache.ProducerCacheEntry producerCacheEntry) {
        return Mono.fromRunnable(producerCacheEntry::releaseLease);
    }

    private <T> Mono<CaffeineReactiveProducerCache.ProducerCacheEntry> leaseCacheEntry(ProducerCacheKey cacheKey,
                                                                                       Mono<Producer<T>> producerMono,
                                                                                       Supplier<PublisherTransformer> producerActionTransformer) {
        return this.getProducerCacheEntry(cacheKey, producerMono, producerActionTransformer)
                .doOnNext(CaffeineReactiveProducerCache.ProducerCacheEntry::activateLease);
    }

    @Override
    public <T, R> Flux<R> usingCachedProducerMany(ProducerCacheKey cacheKey, Mono<Producer<T>> producerMono,
                                                  Supplier<PublisherTransformer> producerActionTransformer,
                                                  Function<Producer<T>, Flux<R>> usingProducerAction) {
        return Flux.usingWhen(this.leaseCacheEntry(cacheKey, producerMono, producerActionTransformer),
                producerCacheEntry -> usingProducerAction.apply(producerCacheEntry.getProducer())
                        .as(producerCacheEntry::decorateProducerAction),
                producerCacheEntry -> this.returnCacheEntry(producerCacheEntry));
    }

    static class ProducerCacheEntry {
        private final AtomicReference<Producer<?>> producer = new AtomicReference();
        private final AtomicReference<Mono<? extends Producer<?>>> producerCreator = new AtomicReference();
        private final AtomicInteger activeLeases = new AtomicInteger(0);
        private final PublisherTransformer producerActionTransformer;
        private volatile boolean removed;

        public ProducerCacheEntry(Producer<?> producer,
                                  Supplier<PublisherTransformer> producerActionTransformer) {
            this.producer.set(producer);
            this.producerCreator.set(Mono.fromSupplier(this.producer::get));
            this.producerActionTransformer = producerActionTransformer != null ? producerActionTransformer.get() :
                    PublisherTransformer.identity();
        }

        void activateLease() {
            this.activeLeases.incrementAndGet();
        }

        void releaseLease() {
            int currentLeases = this.activeLeases.decrementAndGet();
            if (currentLeases == 0 && removed) {
                closeProducer();
            }
        }

        public int getActiveLeases() {
            return this.activeLeases.get();
        }

        public <T> Producer<T> getProducer() {
            return (Producer<T>) this.producer.get();
        }

        <T> Mono<CaffeineReactiveProducerCache.ProducerCacheEntry> recreateIfClosed(Mono<Producer<T>> producerMono) {
            return Mono.defer(() -> {
                Producer<?> p = this.producer.get();
                if (p != null) {
                    if (p.isConnected()) {
                        return Mono.just(this);
                    } else {
                        Mono<? extends Producer<?>> previousUpdater = this.producerCreator.get();
                        if (this.producerCreator.compareAndSet(previousUpdater,
                                createCachedProducerMono(producerMono))) {
                            this.producer.compareAndSet(p, null);
                            CaffeineReactiveProducerCache.flushAndCloseProducerAsync(p);
                        }
                    }
                }
                return Mono.defer(() -> this.producerCreator.get())
                        .filter(Producer::isConnected)
                        .repeatWhenEmpty(5, flux -> flux.delayElements(Duration.ofSeconds(1)))
                        .thenReturn(this);
            });
        }

        private <T> Mono<Producer<T>> createCachedProducerMono(Mono<Producer<T>> producerMono) {
            return producerMono
                    .doOnNext(newProducer -> {
                        log.info("Replaced closed producer for topic {}", newProducer.getTopic());
                        this.producer.set(newProducer);
                    })
                    .cache();
        }

        void close() {
            removed = true;
            if (activeLeases.get() == 0) {
                closeProducer();
            }
            producerActionTransformer.dispose();
        }

        private void closeProducer() {
            Producer<?> p = this.producer.get();
            if (p != null && this.producer.compareAndSet(p, null)) {
                log.info("Closed producer {} for topic {}", p.getProducerName(), p.getTopic());
                flushAndCloseProducerAsync(p);
            }
        }

        <R> Publisher<? extends R> decorateProducerAction(Flux<R> source) {
            return producerActionTransformer.transform(source);
        }

        <R> Mono<? extends R> decorateProducerAction(Mono<R> source) {
            return Mono.from(producerActionTransformer.transform(source));
        }
    }
}
