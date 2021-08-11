package com.github.lhotari.reactive.pulsar.adapter;

public interface ReactiveMessageSenderFactory<T> {
    ReactiveMessageSenderFactory<T> producerConfigurer(ProducerConfigurer<T> producerConfigurer);

    ReactiveMessageSenderFactory<T> topic(String topicName);

    ReactiveMessageSender<T> create();
}