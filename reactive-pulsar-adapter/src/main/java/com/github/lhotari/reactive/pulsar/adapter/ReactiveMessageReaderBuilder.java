package com.github.lhotari.reactive.pulsar.adapter;

public interface ReactiveMessageReaderBuilder<T> {
    ReactiveMessageReaderBuilder<T> readerConfigurer(ReaderConfigurer<T> readerConfigurer);

    ReactiveMessageReaderBuilder<T> topic(String topicName);

    ReactiveMessageReaderBuilder<T> startAtSpec(StartAtSpec startAtSpec);

    ReactiveMessageReaderBuilder<T> endOfStreamAction(EndOfStreamAction endOfStreamAction);

    ReactiveMessageReaderBuilder<T> clone();

    ReactiveMessageReader<T> build();
}
