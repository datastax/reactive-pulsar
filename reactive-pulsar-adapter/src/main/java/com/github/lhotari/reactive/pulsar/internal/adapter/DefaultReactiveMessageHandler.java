package com.github.lhotari.reactive.pulsar.internal.adapter;

import com.github.lhotari.reactive.pulsar.adapter.InflightLimiter;
import com.github.lhotari.reactive.pulsar.adapter.MessageResult;
import com.github.lhotari.reactive.pulsar.adapter.ReactiveMessageConsumer;
import com.github.lhotari.reactive.pulsar.adapter.ReactiveMessageHandler;
import java.time.Duration;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiConsumer;
import java.util.function.Function;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.impl.Murmur3_32Hash;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.Disposable;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;
import reactor.util.context.Context;
import reactor.util.retry.Retry;

class DefaultReactiveMessageHandler<T> implements ReactiveMessageHandler {

    private static final Logger LOG = LoggerFactory.getLogger(DefaultReactiveMessageHandler.class);
    private static final String INFLIGHT_LIMITER_CONTEXT_KEY =
        DefaultReactiveMessageHandlerBuilder.class.getName() + ".INFLIGHT_LIMITER_CONTEXT_KEY";
    private final AtomicReference<Disposable> killSwitch = new AtomicReference<>();
    private final Mono<Void> pipeline;
    private final Function<Message<T>, Mono<Void>> messageHandler;
    private final BiConsumer<Message<T>, Throwable> errorLogger;
    private final Retry pipelineRetrySpec;
    private final Duration handlingTimeout;
    private final Function<Flux<Message<T>>, Flux<MessageResult<Void>>> streamingMessageHandler;
    private final boolean keyOrdered;
    private final int concurrency;
    private final int maxInflight;

    public DefaultReactiveMessageHandler(
        ReactiveMessageConsumer<T> messageConsumer,
        Function<Message<T>, Mono<Void>> messageHandler,
        BiConsumer<Message<T>, Throwable> errorLogger,
        Retry pipelineRetrySpec,
        Duration handlingTimeout,
        Function<Mono<Void>, Mono<Void>> transformer,
        Function<Flux<Message<T>>, Flux<MessageResult<Void>>> streamingMessageHandler,
        boolean keyOrdered,
        int concurrency,
        int maxInflight
    ) {
        this.messageHandler = messageHandler;
        this.errorLogger = errorLogger;
        this.pipelineRetrySpec = pipelineRetrySpec;
        this.handlingTimeout = handlingTimeout;
        this.streamingMessageHandler = streamingMessageHandler;
        this.keyOrdered = keyOrdered;
        this.concurrency = concurrency;
        this.maxInflight = maxInflight;
        this.pipeline =
            messageConsumer
                .consumeMessages(this::createMessageConsumer)
                .then()
                .transform(transformer)
                .transform(this::decoratePipeline);
    }

    private Mono<Void> decorateMessageHandler(Mono<Void> messageHandler) {
        if (handlingTimeout != null) {
            messageHandler = messageHandler.timeout(handlingTimeout);
        }
        if (maxInflight > 0) {
            messageHandler =
                messageHandler.transformDeferredContextual((original, context) -> {
                    InflightLimiter inflightLimiter = context.get(INFLIGHT_LIMITER_CONTEXT_KEY);
                    return inflightLimiter.transform(original);
                });
        }
        return messageHandler;
    }

    private Mono<Void> decoratePipeline(Mono<Void> pipeline) {
        if (maxInflight > 0) {
            Mono<Void> finalPipeline = pipeline;
            pipeline =
                Mono.using(
                    () -> new InflightLimiter(maxInflight),
                    inflightLimiter ->
                        finalPipeline.contextWrite(Context.of(INFLIGHT_LIMITER_CONTEXT_KEY, inflightLimiter)),
                    InflightLimiter::dispose
                );
        }
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
                    "messageHandler and streamingMessageHandler cannot be set at the same time."
                );
            }
            if (concurrency > 1) {
                if (keyOrdered) {
                    return messageFlux
                        .groupBy(message -> resolveGroupKey(message, concurrency))
                        .flatMap(
                            groupedFlux ->
                                groupedFlux
                                    .concatMap(message -> handleMessage(message))
                                    .subscribeOn(Schedulers.parallel()),
                            concurrency
                        );
                } else {
                    return messageFlux.flatMap(
                        message -> handleMessage(message).subscribeOn(Schedulers.parallel()),
                        concurrency
                    );
                }
            } else {
                return messageFlux.concatMap(this::handleMessage);
            }
        } else {
            return Objects
                .requireNonNull(streamingMessageHandler, "streamingMessageHandler or messageHandler must be set")
                .apply(messageFlux);
        }
    }

    private static Integer resolveGroupKey(Message<?> message, int concurrency) {
        byte[] keyBytes = null;
        if (message.hasOrderingKey()) {
            keyBytes = message.getOrderingKey();
        } else if (message.hasKey()) {
            keyBytes = message.getKeyBytes();
        }
        if (keyBytes == null || keyBytes.length == 0) {
            // use a group that has been derived from the message id so that redeliveries get handled in order
            keyBytes = message.getMessageId().toByteArray();
        }
        int keyHash = Murmur3_32Hash.getInstance().makeHash(keyBytes);
        return keyHash % concurrency;
    }

    private Mono<MessageResult<Void>> handleMessage(Message<T> message) {
        return messageHandler
            .apply(message)
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
                    LOG.error("Message handling for message id {} failed.", message.getMessageId(), throwable);
                }
                // TODO: nack doesn't work for batch messages due to Pulsar bugs
                return Mono.just(MessageResult.negativeAcknowledge(message.getMessageId()));
            });
    }

    @Override
    public ReactiveMessageHandler start() {
        if (killSwitch.get() != null) {
            throw new IllegalStateException("Message handler is already running.");
        }
        Disposable disposable = pipeline.subscribe(null, this::logError, this::logUnexpectedCompletion);
        if (!killSwitch.compareAndSet(null, disposable)) {
            disposable.dispose();
            throw new IllegalStateException("Message handler was already running.");
        }
        return this;
    }

    private void logError(Throwable throwable) {
        LOG.error("ReactiveMessageHandler was unexpectedly terminated.", throwable);
    }

    private void logUnexpectedCompletion() {
        if (isRunning()) {
            LOG.error("ReactiveMessageHandler was unexpectedly completed.");
        }
    }

    @Override
    public ReactiveMessageHandler stop() {
        Disposable disposable = killSwitch.getAndSet(null);
        if (disposable != null) {
            disposable.dispose();
        }
        return this;
    }

    @Override
    public boolean isRunning() {
        return killSwitch.get() != null;
    }

    @Override
    public void close() throws Exception {
        stop();
    }
}
