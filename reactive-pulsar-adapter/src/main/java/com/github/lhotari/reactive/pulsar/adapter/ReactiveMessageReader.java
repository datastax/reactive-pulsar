package com.github.lhotari.reactive.pulsar.adapter;

import org.apache.pulsar.client.api.Message;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

public interface ReactiveMessageReader<T> {
    Mono<Message<T>> readMessage(StartAtSpec startAtSpec, EndOfStreamAction endOfStreamAction);
    Flux<Message<T>> readMessages(StartAtSpec startAtSpec, EndOfStreamAction endOfStreamAction);
}
