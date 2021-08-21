package com.github.lhotari.reactive.pulsar.adapter;

import static com.github.lhotari.reactive.pulsar.adapter.PulsarFutureAdapter.adaptPulsarFuture;
import java.util.function.Function;
import java.util.function.Supplier;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.ConsumerBuilder;
import org.apache.pulsar.client.api.PulsarClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

class DefaultReactiveConsumerAdapter<T> implements ReactiveConsumerAdapter<T> {
    private Logger LOG = LoggerFactory.getLogger(DefaultReactiveConsumerAdapter.class);
    private final Supplier<PulsarClient> pulsarClientSupplier;
    private final Function<PulsarClient, ConsumerBuilder<T>> consumerBuilderFactory;

    public DefaultReactiveConsumerAdapter(Supplier<PulsarClient> pulsarClientSupplier,
                                          Function<PulsarClient, ConsumerBuilder<T>> consumerBuilderFactory) {
        this.pulsarClientSupplier = pulsarClientSupplier;
        this.consumerBuilderFactory = consumerBuilderFactory;
    }

    private Mono<Consumer<T>> createConsumerMono() {
        return adaptPulsarFuture(() -> consumerBuilderFactory.apply(pulsarClientSupplier.get()).subscribeAsync());
    }

    private Mono<Void> closeConsumer(Consumer<?> consumer) {
        return Mono.fromFuture(consumer::closeAsync)
                .doOnSuccess(__ -> {
                    LOG.info("Consumer closed {}", consumer);
                });
    }

    @Override
    public <R> Mono<R> usingConsumer(Function<Consumer<T>, Mono<R>> usingConsumerAction) {
        return Mono.usingWhen(createConsumerMono(),
                usingConsumerAction,
                this::closeConsumer);
    }

    @Override
    public <R> Flux<R> usingConsumerMany(Function<Consumer<T>, Flux<R>> usingConsumerAction) {
        return Flux.usingWhen(createConsumerMono(),
                usingConsumerAction,
                this::closeConsumer);
    }
}
