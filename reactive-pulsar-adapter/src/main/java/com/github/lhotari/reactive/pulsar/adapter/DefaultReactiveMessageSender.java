package com.github.lhotari.reactive.pulsar.adapter;

import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.ProducerBuilder;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.api.TypedMessageBuilder;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

class DefaultReactiveMessageSender<T>
        implements ReactiveMessageSender<T> {
    private final Schema<T> schema;
    private final ProducerConfigurer<T> producerConfigurer;
    private final String topicName;
    private final ReactiveProducerAdapterFactory reactiveProducerAdapterFactory;

    public DefaultReactiveMessageSender(Schema<T> schema, ProducerConfigurer<T> producerConfigurer,
                                        String topicName,
                                        ReactiveProducerAdapterFactory reactiveProducerAdapterFactory) {
        this.schema = schema;
        this.producerConfigurer = producerConfigurer;
        this.topicName = topicName;
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
    public Mono<MessageId> sendMessage(MessageConfigurer<T> messageConfigurer) {
        return createReactiveProducerAdapter()
                .usingProducer(producer ->
                        createMessageMono(messageConfigurer, producer));
    }

    private Mono<MessageId> createMessageMono(MessageConfigurer<T> messageConfigurer, Producer<T> producer) {
        return Mono.fromFuture(() -> {
            TypedMessageBuilder<T> typedMessageBuilder = producer.newMessage();
            messageConfigurer.configure(new MessageBuilderAdapter<>(typedMessageBuilder));
            return typedMessageBuilder.sendAsync();
        });
    }

    @Override
    public Mono<MessageId> sendMessagePayload(T payload) {
        return sendMessage(messageBuilder -> messageBuilder.value(payload));
    }

    @Override
    public Flux<MessageId> sendMessages(Flux<MessageConfigurer<T>> messageConfigurers) {
        return createReactiveProducerAdapter()
                .usingProducerMany(producer ->
                        messageConfigurers.concatMap(messageConfigurer ->
                                createMessageMono(messageConfigurer, producer)));
    }

    @Override
    public Flux<MessageId> sendMessagePayloads(Flux<T> payloads) {
        return Flux.defer(() -> sendMessages(payloads.map(payload ->
                messageBuilder -> messageBuilder.value(payload))));
    }
}
