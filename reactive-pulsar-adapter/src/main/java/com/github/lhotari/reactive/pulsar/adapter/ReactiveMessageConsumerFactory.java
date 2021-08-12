package com.github.lhotari.reactive.pulsar.adapter;

public interface ReactiveMessageConsumerFactory<T> {
    ReactiveMessageConsumerFactory<T> consumerConfigurer(ConsumerConfigurer<T> readerConfigurer);

    ReactiveMessageConsumerFactory<T> topic(String topicName);

    ReactiveMessageConsumer<T> create();
}
