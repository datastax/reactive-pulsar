package com.github.lhotari.reactive.pulsar.internal.adapter;

import com.github.lhotari.reactive.pulsar.adapter.InflightLimiter;
import com.github.lhotari.reactive.pulsar.adapter.ProducerConfigurer;
import com.github.lhotari.reactive.pulsar.adapter.ReactiveMessageSender;
import com.github.lhotari.reactive.pulsar.adapter.ReactiveMessageSenderBuilder;
import com.github.lhotari.reactive.pulsar.resourcewrapper.PublisherTransformer;
import com.github.lhotari.reactive.pulsar.resourcewrapper.ReactiveProducerAdapterFactory;
import com.github.lhotari.reactive.pulsar.resourcewrapper.ReactiveProducerCache;
import java.util.function.Supplier;
import org.apache.pulsar.client.api.Schema;
import reactor.core.scheduler.Schedulers;

class DefaultReactiveMessageSenderBuilder<T> implements ReactiveMessageSenderBuilder<T> {

    private final Schema<T> schema;
    private final ReactiveProducerAdapterFactory reactiveProducerAdapterFactory;
    private ProducerConfigurer<T> producerConfigurer;
    private String topicName;
    private ReactiveProducerCache producerCache;
    private int maxInflight = 100;
    private Supplier<PublisherTransformer> producerActionTransformer = PublisherTransformer::identity;

    public DefaultReactiveMessageSenderBuilder(
        Schema<T> schema,
        ReactiveProducerAdapterFactory reactiveProducerAdapterFactory
    ) {
        this.schema = schema;
        this.reactiveProducerAdapterFactory = reactiveProducerAdapterFactory;
    }

    @Override
    public ReactiveMessageSenderBuilder<T> cache(ReactiveProducerCache producerCache) {
        this.producerCache = producerCache;
        return this;
    }

    @Override
    public ReactiveMessageSenderBuilder<T> producerConfigurer(ProducerConfigurer<T> producerConfigurer) {
        this.producerConfigurer = producerConfigurer;
        return this;
    }

    @Override
    public ReactiveMessageSenderBuilder<T> topic(String topicName) {
        this.topicName = topicName;
        return this;
    }

    @Override
    public ReactiveMessageSenderBuilder<T> maxInflight(int maxInflight) {
        this.maxInflight = maxInflight;
        producerActionTransformer =
            () -> new InflightLimiter(maxInflight, Math.max(maxInflight / 2, 1), Schedulers.single());
        return this;
    }

    @Override
    public ReactiveMessageSender<T> build() {
        return new DefaultReactiveMessageSender<>(
            schema,
            producerConfigurer,
            topicName,
            maxInflight,
            reactiveProducerAdapterFactory,
            producerCache,
            producerActionTransformer
        );
    }
}
