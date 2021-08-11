package com.github.lhotari.reactive.pulsar.adapter;

import java.time.Duration;
import java.util.function.BiConsumer;
import java.util.function.Function;
import org.apache.pulsar.client.api.Message;
import reactor.core.publisher.Mono;
import reactor.util.retry.Retry;

public interface ReactiveConsumerPipelineBuilder<T> {
    ReactiveConsumerPipelineBuilder<T> consumerConfigurer(ConsumerConfigurer<T> consumerConfigurer);

    ReactiveConsumerPipelineBuilder<T> messageHandler(Function<Message<T>, Mono<Void>> messageHandler);

    ReactiveConsumerPipelineBuilder<T> errorLogger(BiConsumer<Message<T>, Throwable> errorLogger);

    ReactiveConsumerPipelineBuilder<T> consumeLoopRetrySpec(Retry consumeLoopRetrySpec);

    ReactiveConsumerPipelineBuilder<T> pipelineRetrySpec(Retry pipelineRetrySpec);

    ReactiveConsumerPipelineBuilder<T> handlingTimeout(Duration handlingTimeout);

    ReactiveConsumerPipeline build();
}
