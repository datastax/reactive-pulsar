package com.github.lhotari.reactive.pulsar.adapter;

import org.apache.pulsar.client.api.MessageId;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

public interface ReactiveMessageSender<T> {
    Mono<MessageId> sendMessage(MessageConfigurer<T> messageConfigurer);
    Mono<MessageId> sendMessagePayload(T payload);
    Flux<MessageId> sendMessages(Flux<MessageConfigurer<T>> messageConfigurers);
    Flux<MessageId> sendMessagePayloads(Flux<T> payloads);
}
