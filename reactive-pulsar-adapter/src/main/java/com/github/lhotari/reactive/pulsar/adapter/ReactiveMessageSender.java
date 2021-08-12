package com.github.lhotari.reactive.pulsar.adapter;

import org.apache.pulsar.client.api.MessageId;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

public interface ReactiveMessageSender<T> {
    Mono<MessageId> sendMessage(Mono<MessageSpec<T>> messageSpec);
    Flux<MessageId> sendMessages(Flux<MessageSpec<T>> messageSpecs);
}
