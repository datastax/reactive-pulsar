package com.github.lhotari.reactive.pulsar.adapter;

import com.github.lhotari.reactive.pulsar.resourceadapter.ReactiveProducerCache;

public interface ReactiveMessageSenderBuilder<T> {
    ReactiveMessageSenderBuilder<T> cache(ReactiveProducerCache producerCache);

    ReactiveMessageSenderBuilder<T> producerConfigurer(ProducerConfigurer<T> producerConfigurer);

    ReactiveMessageSenderBuilder<T> topic(String topicName);

    ReactiveMessageSenderBuilder<T> maxInflight(int maxInflight);

    ReactiveMessageSenderBuilder<T> maxConcurrentSenderSubscriptions(int maxConcurrentSenderSubscriptions);

    ReactiveMessageSender<T> build();
}
