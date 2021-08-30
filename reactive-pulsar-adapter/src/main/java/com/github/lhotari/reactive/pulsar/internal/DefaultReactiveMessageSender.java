package com.github.lhotari.reactive.pulsar.internal;

import com.github.lhotari.reactive.pulsar.adapter.*;
import org.apache.pulsar.client.api.*;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

class DefaultReactiveMessageSender<T> implements ReactiveMessageSender<T> {

    private final Schema<T> schema;
    private final ProducerConfigurer<T> producerConfigurer;
    private final String topicName;
    private final int maxInflight;
    private final ReactiveProducerAdapterFactory reactiveProducerAdapterFactory;

    public DefaultReactiveMessageSender(
        Schema<T> schema,
        ProducerConfigurer<T> producerConfigurer,
        String topicName,
        int maxInflight,
        ReactiveProducerAdapterFactory reactiveProducerAdapterFactory
    ) {
        this.schema = schema;
        this.producerConfigurer = producerConfigurer;
        this.topicName = topicName;
        this.maxInflight = maxInflight;
        this.reactiveProducerAdapterFactory = reactiveProducerAdapterFactory;
    }

    ReactiveProducerAdapter<T> createReactiveProducerAdapter() {
        return reactiveProducerAdapterFactory.create(pulsarClient -> {
            ProducerBuilder<T> producerBuilder = pulsarClient.newProducer(schema);
            if (topicName != null) {
                producerBuilder.topic(topicName);
            }
            if (producerConfigurer != null) {
                producerConfigurer.configure(producerBuilder);
            }
            return producerBuilder;
        });
    }

    @Override
    public Mono<MessageId> sendMessage(Mono<MessageSpec<T>> messageSpec) {
        return createReactiveProducerAdapter()
            .usingProducer(producer -> messageSpec.flatMap(m -> createMessageMono(m, producer)));
    }

    private Mono<MessageId> createMessageMono(MessageSpec<T> messageSpec, Producer<T> producer) {
        return PulsarFutureAdapter.adaptPulsarFuture(() -> {
            TypedMessageBuilder<T> typedMessageBuilder = producer.newMessage();
            messageSpec.configure(typedMessageBuilder);
            return typedMessageBuilder.sendAsync();
        });
    }

    @Override
    public Flux<MessageId> sendMessages(Flux<MessageSpec<T>> messageSpecs) {
        return createReactiveProducerAdapter()
            .usingProducerMany(producer ->
                // TODO: ensure that inner publishers are subscribed in order so that message order is retained
                messageSpecs.flatMapSequential(messageSpec -> createMessageMono(messageSpec, producer), maxInflight)
            );
    }
}
