package com.github.lhotari.reactive.pulsar.adapter;

import org.apache.pulsar.client.api.ConsumerBuilder;

public interface ConsumerConfigurer<T> {
    void configure(ConsumerBuilder<T> consumerBuilder);
}
