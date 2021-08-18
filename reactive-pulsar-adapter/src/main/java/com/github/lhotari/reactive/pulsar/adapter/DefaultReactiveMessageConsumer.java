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

class DefaultReactiveMessageConsumer<T> implements ReactiveMessageConsumer<T> {
    private final ReactiveConsumerAdapterFactory reactiveConsumerAdapterFactory;
    private final Schema<T> schema;
    private final ConsumerConfigurer<T> consumerConfigurer;
    private final String topicName;

    public DefaultReactiveMessageConsumer(ReactiveConsumerAdapterFactory reactiveConsumerAdapterFactory,
                                          Schema<T> schema,
                                          ConsumerConfigurer<T> consumerConfigurer, String topicName) {
        this.reactiveConsumerAdapterFactory = reactiveConsumerAdapterFactory;
        this.schema = schema;
        this.consumerConfigurer = consumerConfigurer;
        this.topicName = topicName;
    }

    @Override
    public <R> Mono<R> consumeMessage(Function<Mono<Message<T>>, Mono<MessageResult<R>>> messageHandler) {
        return createReactiveConsumerAdapter().usingConsumer(consumer ->
                messageHandler.apply(readNextMessage(consumer))
                        .delayUntil(messageResult -> handleAcknowledgement(consumer, messageResult))
                        .handle(this::handleMessageResult));
    }

    private <R> Mono<?> handleAcknowledgement(Consumer<T> consumer, MessageResult<R> messageResult) {
        if (messageResult.getMessageId() != null) {
            if (messageResult.isAcknowledgeMessage()) {
                return Mono.fromFuture(
                        () -> consumer.acknowledgeAsync(messageResult.getMessageId()));
            } else {
                return Mono.fromRunnable(
                        () -> consumer.negativeAcknowledge(messageResult.getMessageId()));
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
        return createReactiveConsumerAdapter().usingConsumerMany(
                consumer -> messageHandler.apply(readNextMessage(consumer).repeat())
                        .delayUntil(messageResult -> handleAcknowledgement(consumer, messageResult))
                        .handle(this::handleMessageResult));
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
