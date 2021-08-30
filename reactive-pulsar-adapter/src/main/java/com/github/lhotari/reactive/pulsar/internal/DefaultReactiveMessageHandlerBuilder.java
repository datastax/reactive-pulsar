package com.github.lhotari.reactive.pulsar.internal;

import com.github.lhotari.reactive.pulsar.adapter.MessageResult;
import com.github.lhotari.reactive.pulsar.adapter.ReactiveMessageConsumer;
import com.github.lhotari.reactive.pulsar.adapter.ReactiveMessageHandler;
import com.github.lhotari.reactive.pulsar.adapter.ReactiveMessageHandlerBuilder;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Objects;
import java.util.function.BiConsumer;
import java.util.function.Function;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.impl.Murmur3_32Hash;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;
import reactor.util.retry.Retry;

class DefaultReactiveMessageHandlerBuilder<T> implements
        ReactiveMessageHandlerBuilder.ConcurrentOneByOneMessageHandlerBuilder<T> {
    private final Logger LOG = LoggerFactory.getLogger(DefaultReactiveMessageHandlerBuilder.class);
    private final ReactiveMessageConsumer<T> messageConsumer;
    private Function<Message<T>, Mono<Void>> messageHandler;
    private BiConsumer<Message<T>, Throwable> errorLogger;
    private Retry pipelineRetrySpec = Retry.backoff(Long.MAX_VALUE, Duration.ofSeconds(5))
            .maxBackoff(Duration.ofMinutes(1)).doBeforeRetry(retrySignal -> {
                LOG.error("Message handler pipeline failed." +
                                "Retrying to start message handler pipeline, retry #{}",
                        retrySignal.totalRetriesInARow(),
                        retrySignal.failure());
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
            Function<Message<T>, Mono<Void>> messageHandler) {
        this.messageHandler = messageHandler;
        return this;
    }

    @Override
    public ReactiveMessageHandlerBuilder<T> streamingMessageHandler(
            Function<Flux<Message<T>>, Flux<MessageResult<Void>>> streamingMessageHandler) {
        this.streamingMessageHandler = streamingMessageHandler;
        return this;
    }

    @Override
    public ReactiveMessageHandlerBuilder.OneByOneMessageHandlerBuilder<T> errorLogger(
            BiConsumer<Message<T>, Throwable> errorLogger) {
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
        Mono<Void> pipeline = messageConsumer.consumeMessages(this::createMessageConsumer)
                .then()
                .transform(transformer)
                .transform(this::decoratePipeline);
        return new DefaultReactiveMessageHandler(pipeline);
    }

    private Mono<Void> decorateMessageHandler(Mono<Void> messageHandler) {
        if (handlingTimeout != null) {
            return messageHandler.timeout(handlingTimeout);
        } else {
            return messageHandler;
        }
    }

    private Mono<Void> decoratePipeline(Mono<Void> pipeline) {
        if (pipelineRetrySpec != null) {
            return pipeline.retryWhen(pipelineRetrySpec);
        } else {
            return pipeline;
        }
    }

    private Flux<MessageResult<Void>> createMessageConsumer(Flux<Message<T>> messageFlux) {
        if (messageHandler != null) {
            if (streamingMessageHandler != null) {
                throw new IllegalStateException(
                        "messageHandler and streamingMessageHandler cannot be set at the same time.");
            }
            if (concurrency > 1) {
                if (keyOrdered) {
                    return messageFlux.groupBy(message -> resolveGroupKey(message, concurrency))
                            .flatMap(groupedFlux ->
                                            groupedFlux.concatMap(message -> handleMessage(message))
                                                    .subscribeOn(Schedulers.parallel()),
                                    concurrency);
                } else {
                    return messageFlux.flatMap(message -> handleMessage(message)
                                    .subscribeOn(Schedulers.parallel()),
                            concurrency);
                }
            } else {
                return messageFlux.concatMap(this::handleMessage);
            }
        } else {
            return Objects.requireNonNull(streamingMessageHandler,
                            "streamingMessageHandler or messageHandler must be set")
                    .apply(messageFlux);
        }
    }

    private Integer resolveGroupKey(Message<T> message, int concurrency) {
        byte[] keyBytes;
        if (message.hasOrderingKey()) {
            keyBytes = message.getOrderingKey();
        } else if (message.hasKey()) {
            keyBytes = message.getKey().getBytes(StandardCharsets.UTF_8);
        } else {
            keyBytes = message.getMessageId().toByteArray();
        }
        int hash = Murmur3_32Hash.getInstance().makeHash(keyBytes);
        return hash % concurrency;
    }

    private Mono<MessageResult<Void>> handleMessage(Message<T> message) {
        return messageHandler.apply(message)
                .transform(this::decorateMessageHandler)
                .thenReturn(MessageResult.acknowledge(message.getMessageId()))
                .onErrorResume(throwable -> {
                    if (errorLogger != null) {
                        try {
                            errorLogger.accept(message, throwable);
                        } catch (Exception e) {
                            LOG.error("Error in calling error logger", e);
                        }
                    } else {
                        LOG.error("Message handling for message id {} failed.", message.getMessageId(),
                                throwable);
                    }
                    // TODO: nack doesn't work for batch messages due to Pulsar bugs
                    return Mono.just(MessageResult.negativeAcknowledge(message.getMessageId()));
                });
    }
}
