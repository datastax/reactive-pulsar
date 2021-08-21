package com.github.lhotari.reactive.pulsar.adapter;

import java.time.Duration;
import java.util.function.BiConsumer;
import java.util.function.Function;
import org.apache.pulsar.client.api.Message;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Mono;
import reactor.util.retry.Retry;

class DefaultReactiveMessageHandlerBuilder<T> implements
        ReactiveMessageHandlerBuilder.OneByOneMessageHandlerBuilder<T> {
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

    public DefaultReactiveMessageHandlerBuilder(ReactiveMessageConsumer<T> messageConsumer) {
        this.messageConsumer = messageConsumer;
    }

    @Override
    public ReactiveMessageHandlerBuilder.OneByOneMessageHandlerBuilder<T> messageHandler(Function<Message<T>, Mono<Void>> messageHandler) {
        this.messageHandler = messageHandler;
        return this;
    }

    @Override
    public ReactiveMessageHandlerBuilder.OneByOneMessageHandlerBuilder<T> errorLogger(BiConsumer<Message<T>, Throwable> errorLogger) {
        this.errorLogger = errorLogger;
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
        Mono<Void> pipeline = messageConsumer.consumeMessages(messageFlux ->
                        messageFlux.flatMap(message -> messageHandler.apply(message)
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
                                })))
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
}
