package com.github.lhotari.reactive.pulsar.adapter;

import org.apache.pulsar.client.api.ProducerBuilder;

public interface ProducerConfigurer<T> {
    void configure(ProducerBuilder<T> producerBuilder);
}
