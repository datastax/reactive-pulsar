package com.github.lhotari.reactive.pulsar.adapter;

import java.time.Duration;
import java.util.function.BiConsumer;
import java.util.function.Function;
import org.apache.pulsar.client.api.Message;
import reactor.core.publisher.Mono;
import reactor.util.retry.Retry;

public interface ReactiveMessageHandlerBuilder<T> {
    ReactiveMessageHandlerBuilder<T> consumerConfigurer(ConsumerConfigurer<T> consumerConfigurer);

    ReactiveMessageHandlerBuilder<T> messageHandler(Function<Message<T>, Mono<Void>> messageHandler);

    ReactiveMessageHandlerBuilder<T> errorLogger(BiConsumer<Message<T>, Throwable> errorLogger);

    ReactiveMessageHandlerBuilder<T> consumeLoopRetrySpec(Retry consumeLoopRetrySpec);

    ReactiveMessageHandlerBuilder<T> pipelineRetrySpec(Retry pipelineRetrySpec);

    ReactiveMessageHandlerBuilder<T> handlingTimeout(Duration handlingTimeout);

    ReactiveMessageHandler build();
}
