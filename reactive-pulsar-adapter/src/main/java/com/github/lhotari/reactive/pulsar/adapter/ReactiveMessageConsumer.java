package com.github.lhotari.reactive.pulsar.adapter;

import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

public interface ReactiveMessageConsumer<T> {
    Mono<ConsumedMessage<T>> consumeMessage();

    Flux<ConsumedMessage<T>> consumeMessages();

    /**
     * Creates the Pulsar Consumer and immediately closes it.
     * This is useful for creating the Pulsar subscription that is related to the consumer.
     * Nothing happens unless the returned Mono is subscribed.
     *
     * @return a Mono for consuming nothing
     */
    Mono<Void> consumeNothing();
}
