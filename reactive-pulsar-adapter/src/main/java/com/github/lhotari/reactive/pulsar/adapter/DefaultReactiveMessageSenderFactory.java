package com.github.lhotari.reactive.pulsar.adapter;

import org.apache.pulsar.client.api.Schema;

class DefaultReactiveMessageSenderFactory<T> implements ReactiveMessageSenderFactory<T> {
    private final Schema<T> schema;
    private final ReactiveProducerAdapterFactory reactiveProducerAdapterFactory;
    private ProducerConfigurer<T> producerConfigurer;
    private String topicName;
    private ReactiveProducerCache producerCache;

    public DefaultReactiveMessageSenderFactory(Schema<T> schema,
                                               ReactiveProducerAdapterFactory reactiveProducerAdapterFactory) {
        this.schema = schema;
        this.reactiveProducerAdapterFactory = reactiveProducerAdapterFactory;
    }

    @Override
    public ReactiveMessageSenderFactory<T> cache(ReactiveProducerCache producerCache) {
        this.producerCache = producerCache;
        return this;
    }

    @Override
    public ReactiveMessageSenderFactory<T> producerConfigurer(ProducerConfigurer<T> producerConfigurer) {
        this.producerConfigurer = producerConfigurer;
        return this;
    }

    @Override
    public ReactiveMessageSenderFactory<T> topic(String topicName) {
        this.topicName = topicName;
        return this;
    }

    @Override
    public ReactiveMessageSender<T> create() {
        return new DefaultReactiveMessageSender<>(schema, producerConfigurer, topicName, producerCache,
                reactiveProducerAdapterFactory);
    }
}
