package com.github.lhotari.reactive.pulsar.adapter;

public interface ReactiveMessageReaderFactory<T> {
    ReactiveMessageReaderFactory<T> readerConfigurer(ReaderConfigurer<T> readerConfigurer);

    ReactiveMessageReaderFactory<T> topic(String topicName);

    ReactiveMessageReaderFactory<T> startAtSpec(StartAtSpec startAtSpec);

    ReactiveMessageReaderFactory<T> endOfStreamAction(EndOfStreamAction endOfStreamAction);

    ReactiveMessageReader<T> create();
}
