package com.github.lhotari.reactive.pulsar.adapter;

import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

public interface ReactiveMessageConsumer<T> {
    Mono<ConsumedMessage<T>> consumeMessage();

    Flux<ConsumedMessage<T>> consumeMessages();
}
