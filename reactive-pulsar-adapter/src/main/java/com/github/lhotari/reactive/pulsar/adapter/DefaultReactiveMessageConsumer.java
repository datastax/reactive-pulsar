package com.github.lhotari.reactive.pulsar.adapter;

import static com.github.lhotari.reactive.pulsar.adapter.PulsarFutureAdapter.adaptPulsarFuture;
import java.util.function.Function;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.ConsumerBuilder;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.Schema;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.publisher.SynchronousSink;
import reactor.core.scheduler.Scheduler;
import reactor.core.scheduler.Schedulers;

class DefaultReactiveMessageConsumer<T> implements ReactiveMessageConsumer<T> {
    private final ReactiveConsumerAdapterFactory reactiveConsumerAdapterFactory;
    private final Schema<T> schema;
    private final ConsumerConfigurer<T> consumerConfigurer;
    private final String topicName;
    private final boolean acknowledgeAsynchronously;
    private final Scheduler acknowledgeScheduler;

    public DefaultReactiveMessageConsumer(ReactiveConsumerAdapterFactory reactiveConsumerAdapterFactory,
                                          Schema<T> schema,
                                          ConsumerConfigurer<T> consumerConfigurer, String topicName,
                                          boolean acknowledgeAsynchronously,
                                          Scheduler acknowledgeScheduler) {
        this.reactiveConsumerAdapterFactory = reactiveConsumerAdapterFactory;
        this.schema = schema;
        this.consumerConfigurer = consumerConfigurer;
        this.topicName = topicName;
        this.acknowledgeAsynchronously = acknowledgeAsynchronously;
        this.acknowledgeScheduler = acknowledgeScheduler;
    }

    @Override
    public <R> Mono<R> consumeMessage(Function<Mono<Message<T>>, Mono<MessageResult<R>>> messageHandler) {
        return createReactiveConsumerAdapter().usingConsumer(consumer ->
                Mono.using(() -> Schedulers.single(acknowledgeScheduler),
                        pinnedAcknowledgeScheduler ->
                                messageHandler.apply(readNextMessage(consumer))
                                        .delayUntil(messageResult ->
                                                handleAcknowledgement(consumer, messageResult,
                                                        pinnedAcknowledgeScheduler))
                                        .handle(this::handleMessageResult),
                        Scheduler::dispose));
    }

    private <R> Mono<?> handleAcknowledgement(Consumer<T> consumer, MessageResult<R> messageResult,
                                              Scheduler pinnedAcknowledgeScheduler) {
        if (messageResult.getMessageId() != null) {
            Mono<Void> acknowledgementMono;
            if (messageResult.isAcknowledgeMessage()) {
                acknowledgementMono = Mono.fromFuture(
                        () -> consumer.acknowledgeAsync(messageResult.getMessageId()));
            } else {
                acknowledgementMono = Mono.fromRunnable(
                        () -> consumer.negativeAcknowledge(messageResult.getMessageId()));
            }
            acknowledgementMono = acknowledgementMono.subscribeOn(pinnedAcknowledgeScheduler);
            if (acknowledgeAsynchronously) {
                return Mono.fromRunnable(acknowledgementMono::subscribe);
            } else {
                return acknowledgementMono;
            }
        } else {
            return Mono.empty();
        }
    }

    static <T> Mono<Message<T>> readNextMessage(Consumer<T> consumer) {
        return adaptPulsarFuture(consumer::receiveAsync);
    }

    private ReactiveConsumerAdapter<T> createReactiveConsumerAdapter() {
        return reactiveConsumerAdapterFactory.create(pulsarClient -> {
            ConsumerBuilder<T> consumerBuilder = pulsarClient.newConsumer(schema);
            if (topicName != null) {
                consumerBuilder.topic(topicName);
            }
            if (consumerConfigurer != null) {
                consumerConfigurer.configure(consumerBuilder);
            }
            return consumerBuilder;
        });
    }

    @Override
    public <R> Flux<R> consumeMessages(Function<Flux<Message<T>>, Flux<MessageResult<R>>> messageHandler) {
        return createReactiveConsumerAdapter().usingConsumerMany(consumer ->
                Flux.using(() -> Schedulers.single(acknowledgeScheduler),
                        pinnedAcknowledgeScheduler ->
                                messageHandler.apply(readNextMessage(consumer).repeat())
                                        .delayUntil(messageResult -> handleAcknowledgement(consumer, messageResult,
                                                pinnedAcknowledgeScheduler))
                                        .handle(this::handleMessageResult),
                        Scheduler::dispose));
    }

    @Override
    public Mono<Void> consumeNothing() {
        return createReactiveConsumerAdapter().usingConsumer(consumer -> Mono.empty());
    }

    private <R> void handleMessageResult(MessageResult<R> messageResult, SynchronousSink<R> sink) {
        R value = messageResult.getValue();
        if (value != null) {
            sink.next(value);
        }
    }
}
