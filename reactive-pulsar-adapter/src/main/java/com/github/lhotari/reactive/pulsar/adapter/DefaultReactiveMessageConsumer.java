package com.github.lhotari.reactive.pulsar.adapter;

import static com.github.lhotari.reactive.pulsar.adapter.PulsarFutureAdapter.adaptPulsarFuture;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.ConsumerBuilder;
import org.apache.pulsar.client.api.Schema;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

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
    public Mono<ConsumedMessage<T>> consumeMessage() {
        return createReactiveConsumerAdapter().usingConsumer(DefaultReactiveMessageConsumer::readNextMessage);
    }

    static <T> Mono<ConsumedMessage<T>> readNextMessage(Consumer<T> consumer) {
        return adaptPulsarFuture(consumer::receiveAsync)
                .map(message -> new DefaultConsumedMessage(message, consumer));
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
    public Flux<ConsumedMessage<T>> consumeMessages() {
        return createReactiveConsumerAdapter().usingConsumerMany(consumer -> readNextMessage(consumer).repeat());
    }
}
