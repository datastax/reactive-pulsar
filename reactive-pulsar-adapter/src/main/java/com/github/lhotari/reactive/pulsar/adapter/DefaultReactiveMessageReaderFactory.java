package com.github.lhotari.reactive.pulsar.adapter;

import org.apache.pulsar.client.api.Schema;

class DefaultReactiveMessageReaderFactory<T> implements ReactiveMessageReaderFactory<T> {
    private final ReactiveReaderAdapterFactory reactiveReaderAdapterFactory;
    private final Schema<T> schema;
    private ReaderConfigurer<T> readerConfigurer;
    private String topicName;
    private StartAtSpec startAtSpec = StartAtSpec.ofEarliest();
    private EndOfStreamAction endOfStreamAction = EndOfStreamAction.COMPLETE;

    public DefaultReactiveMessageReaderFactory(Schema<T> schema,
                                               ReactiveReaderAdapterFactory reactiveReaderAdapterFactory) {
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
    public ReactiveMessageReaderFactory<T> startAtSpec(StartAtSpec startAtSpec) {
        this.startAtSpec = startAtSpec;
        return this;
    }

    @Override
    public ReactiveMessageReaderFactory<T> endOfStreamAction(EndOfStreamAction endOfStreamAction) {
        this.endOfStreamAction = endOfStreamAction;
        return this;
    }

    @Override
    public ReactiveMessageReaderFactory<T> clone() {
        DefaultReactiveMessageReaderFactory<T> cloned = new DefaultReactiveMessageReaderFactory<>(schema,
                reactiveReaderAdapterFactory);
        cloned.readerConfigurer = this.readerConfigurer;
        cloned.topicName = this.topicName;
        cloned.startAtSpec = this.startAtSpec;
        cloned.endOfStreamAction = this.endOfStreamAction;
        return this;
    }

    @Override
    public ReactiveMessageReader<T> create() {
        return new DefaultReactiveMessageReader<>(reactiveReaderAdapterFactory, schema, readerConfigurer, topicName,
                startAtSpec, endOfStreamAction);
    }
}
