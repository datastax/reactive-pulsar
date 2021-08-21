package com.github.lhotari.reactive.pulsar.adapter;

import java.time.Duration;
import java.util.function.BiConsumer;
import java.util.function.Function;
import org.apache.pulsar.client.api.Message;
import reactor.core.publisher.Mono;
import reactor.util.retry.Retry;

public interface ReactiveMessageHandlerBuilder<T> {
    static <T> ReactiveMessageHandlerBuilder<T> builder(ReactiveMessageConsumer<T> messageConsumer) {
        return new DefaultReactiveMessageHandlerBuilder<>(messageConsumer);
    }

    interface OneByOneMessageHandlerBuilder<T> extends ReactiveMessageHandlerBuilder<T> {
        OneByOneMessageHandlerBuilder<T> handlingTimeout(Duration handlingTimeout);
        OneByOneMessageHandlerBuilder<T> errorLogger(BiConsumer<Message<T>, Throwable> errorLogger);
    }

    OneByOneMessageHandlerBuilder<T> messageHandler(Function<Message<T>, Mono<Void>> messageHandler);

    ReactiveMessageHandlerBuilder<T> transformPipeline(Function<Mono<Void>, Mono<Void>> transformer);

    ReactiveMessageHandlerBuilder<T> pipelineRetrySpec(Retry pipelineRetrySpec);

    ReactiveMessageHandler build();
}
