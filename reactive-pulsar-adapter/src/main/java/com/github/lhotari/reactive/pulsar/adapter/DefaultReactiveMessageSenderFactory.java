package com.github.lhotari.reactive.pulsar.adapter;

import java.util.function.Supplier;
import org.apache.pulsar.client.api.Schema;
import reactor.core.scheduler.Schedulers;

class DefaultReactiveMessageSenderFactory<T> implements ReactiveMessageSenderFactory<T> {
    private final Schema<T> schema;
    private final Supplier<ReactiveProducerAdapterFactory> reactiveProducerAdapterFactorySupplier;
    private ProducerConfigurer<T> producerConfigurer;
    private String topicName;
    private ReactiveProducerCache producerCache;
    private int maxInflight = 100;
    private Supplier<PublisherTransformer> producerActionTransformer = PublisherTransformer::identity;

    public DefaultReactiveMessageSenderFactory(Schema<T> schema,
                                               Supplier<ReactiveProducerAdapterFactory> reactiveProducerAdapterFactorySupplier) {
        this.schema = schema;
        this.reactiveProducerAdapterFactorySupplier = reactiveProducerAdapterFactorySupplier;
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
    public ReactiveMessageSenderFactory<T> maxInflight(int maxInflight) {
        this.maxInflight = maxInflight;
        producerActionTransformer =
                () -> new InflightLimiter(maxInflight, Math.max(maxInflight / 2, 1), Schedulers.single());
        return this;
    }

    @Override
    public ReactiveMessageSender<T> create() {
        ReactiveProducerAdapterFactory reactiveProducerAdapterFactory = reactiveProducerAdapterFactorySupplier.get();
        reactiveProducerAdapterFactory.cache(producerCache);
        reactiveProducerAdapterFactory.producerActionTransformer(producerActionTransformer);
        return new DefaultReactiveMessageSender<>(schema, producerConfigurer, topicName, maxInflight,
                reactiveProducerAdapterFactory);
    }
}
