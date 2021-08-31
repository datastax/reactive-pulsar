package com.github.lhotari.reactive.pulsar.internal.adapter;

import com.github.lhotari.reactive.pulsar.adapter.ProducerConfigurer;
import com.github.lhotari.reactive.pulsar.adapter.ReactiveMessageSender;
import com.github.lhotari.reactive.pulsar.adapter.ReactiveMessageSenderBuilder;
import com.github.lhotari.reactive.pulsar.resourceadapter.PublisherTransformer;
import com.github.lhotari.reactive.pulsar.resourceadapter.ReactiveProducerAdapterFactory;
import com.github.lhotari.reactive.pulsar.resourceadapter.ReactiveProducerCache;
import java.util.function.Supplier;
import org.apache.pulsar.client.api.Schema;
import reactor.core.scheduler.Schedulers;

class DefaultReactiveMessageSenderBuilder<T> implements ReactiveMessageSenderBuilder<T> {

    private final Schema<T> schema;
    private final ReactiveProducerAdapterFactory reactiveProducerAdapterFactory;
    private ProducerConfigurer<T> producerConfigurer;
    private String topicName;
    private ReactiveProducerCache producerCache;
    private int maxInflight = -1;
    private int maxConcurrentSenderSubscriptions = InflightLimiter.DEFAULT_MAX_PENDING_SUBSCRIPTIONS;
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
        return this;
    }

    @Override
    public ReactiveMessageSenderBuilder<T> maxConcurrentSenderSubscriptions(int maxConcurrentSenderSubscriptions) {
        this.maxConcurrentSenderSubscriptions = maxConcurrentSenderSubscriptions;
        return this;
    }

    @Override
    public ReactiveMessageSender<T> build() {
        if (maxInflight > 0) {
            producerActionTransformer =
                () ->
                    new InflightLimiter(
                        maxInflight,
                        Math.max(maxInflight / 2, 1),
                        Schedulers.single(),
                        maxConcurrentSenderSubscriptions
                    );
        }
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
