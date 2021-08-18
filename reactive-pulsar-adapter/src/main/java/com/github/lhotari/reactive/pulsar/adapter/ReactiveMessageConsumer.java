package com.github.lhotari.reactive.pulsar.adapter;

import java.util.function.Function;
import org.apache.pulsar.client.api.Message;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

public interface ReactiveMessageConsumer<T> {
    <R> Mono<R> consumeMessage(Function<Mono<Message<T>>, Mono<MessageResult<R>>> messageHandler);

    <R> Flux<R> consumeMessages(Function<Flux<Message<T>>, Flux<MessageResult<R>>> messageHandler);

    /**
     * Creates the Pulsar Consumer and immediately closes it.
     * This is useful for creating the Pulsar subscription that is related to the consumer.
     * Nothing happens unless the returned Mono is subscribed.
     *
     * @return a Mono for consuming nothing
     */
    Mono<Void> consumeNothing();
}
