package com.github.lhotari.reactive.pulsar.adapter;

import org.apache.pulsar.client.api.Schema;

public class DefaultReactiveMessageConsumerFactory<T> implements ReactiveMessageConsumerFactory<T> {
    private final Schema<T> schema;
    private final ReactiveConsumerAdapterFactory reactiveConsumerAdapterFactory;
    private ConsumerConfigurer<T> readerConfigurer;
    private String topicName;

    public DefaultReactiveMessageConsumerFactory(Schema<T> schema,
                                                 ReactiveConsumerAdapterFactory reactiveConsumerAdapterFactory) {
        this.schema = schema;
        this.reactiveConsumerAdapterFactory = reactiveConsumerAdapterFactory;
    }

    @Override
    public ReactiveMessageConsumerFactory<T> consumerConfigurer(ConsumerConfigurer<T> readerConfigurer) {
        this.readerConfigurer = readerConfigurer;
        return this;
    }

    @Override
    public ReactiveMessageConsumerFactory<T> topic(String topicName) {
        this.topicName = topicName;
        return this;
    }

    @Override
    public ReactiveMessageConsumer<T> create() {
        return new DefaultReactiveMessageConsumer<T>(reactiveConsumerAdapterFactory, schema, readerConfigurer,
                topicName);
    }
}
