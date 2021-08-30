package com.github.lhotari.reactive.pulsar.adapter;

import com.github.lhotari.reactive.pulsar.internal.DefaultImplementationFactory;
import java.time.Duration;
import java.util.function.BiConsumer;
import java.util.function.Function;
import org.apache.pulsar.client.api.Message;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.util.retry.Retry;

public interface ReactiveMessageHandlerBuilder<T> {
    static <T> ReactiveMessageHandlerBuilder<T> builder(ReactiveMessageConsumer<T> messageConsumer) {
        return DefaultImplementationFactory.createReactiveMessageHandlerBuilder(messageConsumer);
    }

    interface OneByOneMessageHandlerBuilder<T> extends ReactiveMessageHandlerBuilder<T> {
        OneByOneMessageHandlerBuilder<T> handlingTimeout(Duration handlingTimeout);

        OneByOneMessageHandlerBuilder<T> errorLogger(BiConsumer<Message<T>, Throwable> errorLogger);

        ConcurrentOneByOneMessageHandlerBuilder<T> concurrent();
    }

    interface ConcurrentOneByOneMessageHandlerBuilder<T> extends OneByOneMessageHandlerBuilder<T> {
        ConcurrentOneByOneMessageHandlerBuilder<T> keyOrdered(boolean keyOrdered);

        ConcurrentOneByOneMessageHandlerBuilder<T> concurrency(int concurrency);

        ConcurrentOneByOneMessageHandlerBuilder<T> maxInflight(int maxInflight);
    }

    OneByOneMessageHandlerBuilder<T> messageHandler(Function<Message<T>, Mono<Void>> messageHandler);

    ReactiveMessageHandlerBuilder<T> streamingMessageHandler(
        Function<Flux<Message<T>>, Flux<MessageResult<Void>>> streamingMessageHandler
    );

    ReactiveMessageHandlerBuilder<T> transformPipeline(Function<Mono<Void>, Mono<Void>> transformer);

    ReactiveMessageHandlerBuilder<T> pipelineRetrySpec(Retry pipelineRetrySpec);

    ReactiveMessageHandler build();
}
