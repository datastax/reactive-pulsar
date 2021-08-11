package com.github.lhotari.reactive.pulsar.adapter;

import org.apache.pulsar.client.api.ReaderBuilder;

public interface ReaderConfigurer<T> {
    void configure(ReaderBuilder<T> readerBuilderBuilder);
}
