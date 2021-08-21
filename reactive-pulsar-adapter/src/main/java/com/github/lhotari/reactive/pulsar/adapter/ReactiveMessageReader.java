package com.github.lhotari.reactive.pulsar.adapter;

import org.apache.pulsar.client.api.Message;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

public interface ReactiveMessageReader<T> {
    Mono<Message<T>> readMessage();

    Flux<Message<T>> readMessages();
}
