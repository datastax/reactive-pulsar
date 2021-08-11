package com.github.lhotari.reactive.pulsar.adapter;

import org.apache.pulsar.client.api.Schema;

class DefaultReactiveMessageReaderFactory<T> implements ReactiveMessageReaderFactory<T> {
    private final ReactiveReaderAdapterFactory reactiveReaderAdapterFactory;
    private final Schema<T> schema;
    private ReaderConfigurer<T> readerConfigurer;
    private String topicName;

    public DefaultReactiveMessageReaderFactory(ReactiveReaderAdapterFactory reactiveReaderAdapterFactory,
                                               Schema<T> schema) {
        this.reactiveReaderAdapterFactory = reactiveReaderAdapterFactory;
        this.schema = schema;
    }

    @Override
    public ReactiveMessageReaderFactory<T> readerConfigurer(ReaderConfigurer<T> readerConfigurer) {
        this.readerConfigurer = readerConfigurer;
        return this;
    }

    @Override
    public ReactiveMessageReaderFactory<T> topic(String topicName) {
        this.topicName = topicName;
        return this;
    }

    @Override
    public ReactiveMessageReader<T> create() {
        return new DefaultReactiveMessageReader<>(schema, readerConfigurer, topicName, reactiveReaderAdapterFactory);
    }
}