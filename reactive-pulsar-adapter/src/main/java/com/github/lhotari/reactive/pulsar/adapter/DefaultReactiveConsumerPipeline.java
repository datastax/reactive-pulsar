package com.github.lhotari.reactive.pulsar.adapter;

import static com.github.lhotari.reactive.pulsar.adapter.PulsarFutureAdapter.adaptPulsarFuture;
import java.time.Duration;
import java.util.Objects;
import java.util.function.BiConsumer;
import java.util.function.Function;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.ConsumerBuilder;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.Schema;
import reactor.core.Disposable;
import reactor.core.publisher.Mono;
import reactor.util.retry.Retry;

class DefaultReactiveConsumerPipeline<T>
        implements ReactiveConsumerPipeline {
    private final Schema<T> schema;
    private final ConsumerConfigurer<T> consumerConfigurer;
    private final Function<Message<T>, Mono<Void>> messageHandler;
    private final BiConsumer<Message<T>, Throwable> errorLogger;
    private final Retry consumeLoopRetrySpec;
    private final Retry pipelineRetrySpec;
    private final Duration handlingTimeout;
    private final ReactiveConsumerAdapterFactory reactiveConsumerAdapterFactory;
    private final Disposable killSwitch;

    public DefaultReactiveConsumerPipeline(DefaultReactiveConsumerPipelineBuilder<T> builder) {
        this.schema = builder.getSchema();
        this.consumerConfigurer = Objects.requireNonNull(builder.getConsumerConfigurer());
        this.messageHandler = Objects.requireNonNull(builder.getMessageHandler());
        this.errorLogger = builder.getErrorLogger();
        this.consumeLoopRetrySpec = builder.getConsumeLoopRetrySpec();
        this.pipelineRetrySpec = builder.getPipelineRetrySpec();
        this.handlingTimeout = builder.getHandlingTimeout();
        this.reactiveConsumerAdapterFactory = builder.getReactiveConsumerAdapterFactory();
        killSwitch = start();
    }

    private Disposable start() {
        return createPipeline()
                .subscribe();
    }

    private Mono<Void> createPipeline() {
        ReactiveConsumerAdapter<T> reactiveConsumerAdapter = reactiveConsumerAdapterFactory.create(pulsarClient -> {
            ConsumerBuilder<T> consumerBuilder = pulsarClient.newConsumer(schema);
            consumerConfigurer.configure(consumerBuilder);
            return consumerBuilder;
        });
        return consumeMessages(reactiveConsumerAdapter);
    }

    private Mono<Void> consumeMessages(ReactiveConsumerAdapter<T> reactiveConsumerAdapter) {
        return reactiveConsumerAdapter.usingConsumer(consumer ->
                        adaptPulsarFuture(consumer::receiveAsync)
                                .flatMap(message -> messageHandler.apply(message)
                                        .transform(this::decorateMessageHandler)
                                        .thenReturn(message)
                                        .onErrorResume(throwable -> {
                                            if (errorLogger != null) {
                                                errorLogger.accept(message, throwable);
                                            }
                                            // TODO: nack doesn't work for batch messages due to Pulsar bugs
                                            consumer.negativeAcknowledge(message);
                                            return Mono.empty();
                                        })
                                )
                                .flatMap(message -> acknowledgeMessage(consumer, message))
                                .repeat()
                                .then()
                                .transform(this::decorateConsumeLoop))
                .transform(this::decoratePipeline);
    }

    private Mono<Void> decorateMessageHandler(Mono<Void> messageHandler) {
        if (handlingTimeout != null) {
            return messageHandler.timeout(handlingTimeout);
        } else {
            return messageHandler;
        }
    }

    private Mono<?> acknowledgeMessage(Consumer<T> consumer, Message<T> message) {
        return Mono.fromRunnable(() -> consumer.acknowledgeAsync(message));
    }

    private Mono<Void> decorateConsumeLoop(Mono<Void> consumeLoop) {
        if (consumeLoopRetrySpec != null) {
            return consumeLoop.retryWhen(consumeLoopRetrySpec);
        } else {
            return consumeLoop;
        }
    }

    private Mono<Void> decoratePipeline(Mono<Void> pipeline) {
        if (pipelineRetrySpec != null) {
            return pipeline.retryWhen(pipelineRetrySpec);
        } else {
            return pipeline;
        }
    }

    @Override
    public void close() throws Exception {
        killSwitch.dispose();
    }
}
