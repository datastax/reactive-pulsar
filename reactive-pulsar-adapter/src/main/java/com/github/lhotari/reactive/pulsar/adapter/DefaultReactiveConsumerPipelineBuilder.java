package com.github.lhotari.reactive.pulsar.adapter;

import java.time.Duration;
import java.util.function.BiConsumer;
import java.util.function.Function;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.Schema;
import reactor.core.publisher.Mono;
import reactor.util.retry.Retry;

class DefaultReactiveConsumerPipelineBuilder<T> implements ReactiveConsumerPipelineBuilder<T> {
    private final Schema<T> schema;
    private final ReactiveConsumerAdapterFactory reactiveConsumerAdapterFactory;
    private ConsumerConfigurer<T> consumerConfigurer;
    private Function<Message<T>, Mono<Void>> messageHandler;
    private BiConsumer<Message<T>, Throwable> errorLogger;
    private Retry consumeLoopRetrySpec = Retry.backoff(10, Duration.ofSeconds(1))
            .maxBackoff(Duration.ofSeconds(30));
    private Retry pipelineRetrySpec = Retry.backoff(Long.MAX_VALUE, Duration.ofSeconds(5))
            .maxBackoff(Duration.ofMinutes(1));
    private Duration handlingTimeout = Duration.ofSeconds(120);

    public DefaultReactiveConsumerPipelineBuilder(Schema<T> schema,
                                                  ReactiveConsumerAdapterFactory reactiveConsumerAdapterFactory) {
        this.schema = schema;
        this.reactiveConsumerAdapterFactory = reactiveConsumerAdapterFactory;
    }

    @Override
    public ReactiveConsumerPipelineBuilder<T> consumerConfigurer(ConsumerConfigurer<T> consumerConfigurer) {
        this.consumerConfigurer = consumerConfigurer;
        return this;
    }

    @Override
    public ReactiveConsumerPipelineBuilder<T> messageHandler(Function<Message<T>, Mono<Void>> messageHandler) {
        this.messageHandler = messageHandler;
        return this;
    }

    @Override
    public ReactiveConsumerPipelineBuilder<T> errorLogger(BiConsumer<Message<T>, Throwable> errorLogger) {
        this.errorLogger = errorLogger;
        return this;
    }

    @Override
    public ReactiveConsumerPipelineBuilder<T> consumeLoopRetrySpec(Retry consumeLoopRetrySpec) {
        this.consumeLoopRetrySpec = consumeLoopRetrySpec;
        return this;
    }

    @Override
    public ReactiveConsumerPipelineBuilder<T> pipelineRetrySpec(Retry pipelineRetrySpec) {
        this.pipelineRetrySpec = pipelineRetrySpec;
        return this;
    }

    @Override
    public ReactiveConsumerPipelineBuilder<T> handlingTimeout(Duration handlingTimeout) {
        this.handlingTimeout = handlingTimeout;
        return this;
    }

    @Override
    public ReactiveConsumerPipeline build() {
        return new DefaultReactiveConsumerPipeline<T>(this);
    }

    public Schema<T> getSchema() {
        return schema;
    }

    public ConsumerConfigurer<T> getConsumerConfigurer() {
        return consumerConfigurer;
    }

    public Function<Message<T>, Mono<Void>> getMessageHandler() {
        return messageHandler;
    }

    public BiConsumer<Message<T>, Throwable> getErrorLogger() {
        return errorLogger;
    }

    public Retry getConsumeLoopRetrySpec() {
        return consumeLoopRetrySpec;
    }

    public Retry getPipelineRetrySpec() {
        return pipelineRetrySpec;
    }

    public Duration getHandlingTimeout() {
        return handlingTimeout;
    }

    public ReactiveConsumerAdapterFactory getReactiveConsumerAdapterFactory() {
        return reactiveConsumerAdapterFactory;
    }
}
