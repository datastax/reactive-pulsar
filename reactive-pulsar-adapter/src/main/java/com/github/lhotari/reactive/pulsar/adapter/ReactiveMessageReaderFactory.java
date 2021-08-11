package com.github.lhotari.reactive.pulsar.adapter;

public interface ReactiveMessageReaderFactory<T> {
    ReactiveMessageReaderFactory<T> readerConfigurer(ReaderConfigurer<T> readerConfigurer);

    ReactiveMessageReaderFactory<T> topic(String topicName);

    ReactiveMessageReader<T> create();
}
