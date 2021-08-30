package com.github.lhotari.reactive.pulsar.internal.adapter;

import com.github.lhotari.reactive.pulsar.adapter.MessageResult;
import com.github.lhotari.reactive.pulsar.adapter.ReactiveMessageConsumer;
import com.github.lhotari.reactive.pulsar.adapter.ReactiveMessageHandler;
import com.github.lhotari.reactive.pulsar.adapter.ReactiveMessageHandlerBuilder;
import java.time.Duration;
import java.util.function.BiConsumer;
import java.util.function.Function;
import org.apache.pulsar.client.api.Message;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.util.retry.Retry;

class DefaultReactiveMessageHandlerBuilder<T>
    implements ReactiveMessageHandlerBuilder.ConcurrentOneByOneMessageHandlerBuilder<T> {

    private final Logger LOG = LoggerFactory.getLogger(DefaultReactiveMessageHandlerBuilder.class);
    private final ReactiveMessageConsumer<T> messageConsumer;
    private Function<Message<T>, Mono<Void>> messageHandler;
    private BiConsumer<Message<T>, Throwable> errorLogger;
    private Retry pipelineRetrySpec = Retry
        .backoff(Long.MAX_VALUE, Duration.ofSeconds(5))
        .maxBackoff(Duration.ofMinutes(1))
        .doBeforeRetry(retrySignal -> {
            LOG.error(
                "Message handler pipeline failed." + "Retrying to start message handler pipeline, retry #{}",
                retrySignal.totalRetriesInARow(),
                retrySignal.failure()
            );
        });
    private Duration handlingTimeout = Duration.ofSeconds(120);
    private Function<Mono<Void>, Mono<Void>> transformer = Function.identity();
    private Function<Flux<Message<T>>, Flux<MessageResult<Void>>> streamingMessageHandler;
    private boolean keyOrdered;
    private int concurrency;
    private int maxInflight;

    public DefaultReactiveMessageHandlerBuilder(ReactiveMessageConsumer<T> messageConsumer) {
        this.messageConsumer = messageConsumer;
    }

    @Override
    public ReactiveMessageHandlerBuilder.OneByOneMessageHandlerBuilder<T> messageHandler(
        Function<Message<T>, Mono<Void>> messageHandler
    ) {
        this.messageHandler = messageHandler;
        return this;
    }

    @Override
    public ReactiveMessageHandlerBuilder<T> streamingMessageHandler(
        Function<Flux<Message<T>>, Flux<MessageResult<Void>>> streamingMessageHandler
    ) {
        this.streamingMessageHandler = streamingMessageHandler;
        return this;
    }

    @Override
    public ReactiveMessageHandlerBuilder.OneByOneMessageHandlerBuilder<T> errorLogger(
        BiConsumer<Message<T>, Throwable> errorLogger
    ) {
        this.errorLogger = errorLogger;
        return this;
    }

    @Override
    public ConcurrentOneByOneMessageHandlerBuilder<T> concurrent() {
        return this;
    }

    @Override
    public ConcurrentOneByOneMessageHandlerBuilder<T> keyOrdered(boolean keyOrdered) {
        this.keyOrdered = keyOrdered;
        return this;
    }

    @Override
    public ConcurrentOneByOneMessageHandlerBuilder<T> concurrency(int concurrency) {
        this.concurrency = concurrency;
        return this;
    }

    @Override
    public ConcurrentOneByOneMessageHandlerBuilder<T> maxInflight(int maxInflight) {
        this.maxInflight = maxInflight;
        return this;
    }

    @Override
    public ReactiveMessageHandlerBuilder.OneByOneMessageHandlerBuilder<T> handlingTimeout(Duration handlingTimeout) {
        this.handlingTimeout = handlingTimeout;
        return this;
    }

    @Override
    public ReactiveMessageHandlerBuilder<T> pipelineRetrySpec(Retry pipelineRetrySpec) {
        this.pipelineRetrySpec = pipelineRetrySpec;
        return this;
    }

    @Override
    public ReactiveMessageHandlerBuilder<T> transformPipeline(Function<Mono<Void>, Mono<Void>> transformer) {
        this.transformer = transformer;
        return this;
    }

    @Override
    public ReactiveMessageHandler build() {
        return new DefaultReactiveMessageHandler(
            messageConsumer,
            messageHandler,
            errorLogger,
            pipelineRetrySpec,
            handlingTimeout,
            transformer,
            streamingMessageHandler,
            keyOrdered,
            concurrency,
            maxInflight
        );
    }
}
