package com.github.lhotari.reactive.pulsar.adapter;

import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.Message;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;

class DefaultConsumedMessage<T> implements ConsumedMessage<T> {
    private static final Logger LOG = LoggerFactory.getLogger(DefaultConsumedMessage.class);
    private final Message<T> message;
    private final Consumer<T> consumer;

    DefaultConsumedMessage(Message<T> message, Consumer<T> consumer) {
        this.message = message;
        this.consumer = consumer;
    }

    public Message<T> getMessage() {
        return message;
    }

    @Override
    public void acknowledge() {
        Mono.fromFuture(() -> consumer.acknowledgeAsync(message))
                .subscribeOn(Schedulers.boundedElastic())
                .doOnError(throwable -> {
                    LOG.error("Failed to acknowledge message {}", message, throwable);
                })
                .subscribe();
    }

    @Override
    public void negativeAcknowledge() {
        consumer.negativeAcknowledge(message);
    }
}
