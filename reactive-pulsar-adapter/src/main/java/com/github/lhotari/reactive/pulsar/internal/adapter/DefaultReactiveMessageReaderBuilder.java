package com.github.lhotari.reactive.pulsar.internal.adapter;

import com.github.lhotari.reactive.pulsar.adapter.*;
import com.github.lhotari.reactive.pulsar.resourcewrapper.ReactiveReaderAdapterFactory;
import org.apache.pulsar.client.api.Schema;

class DefaultReactiveMessageReaderBuilder<T> implements ReactiveMessageReaderBuilder<T> {

    private final ReactiveReaderAdapterFactory reactiveReaderAdapterFactory;
    private final Schema<T> schema;
    private ReaderConfigurer<T> readerConfigurer;
    private String topicName;
    private StartAtSpec startAtSpec = StartAtSpec.ofEarliest();
    private EndOfStreamAction endOfStreamAction = EndOfStreamAction.COMPLETE;

    public DefaultReactiveMessageReaderBuilder(
        Schema<T> schema,
        ReactiveReaderAdapterFactory reactiveReaderAdapterFactory
    ) {
        this.reactiveReaderAdapterFactory = reactiveReaderAdapterFactory;
        this.schema = schema;
    }

    @Override
    public ReactiveMessageReaderBuilder<T> readerConfigurer(ReaderConfigurer<T> readerConfigurer) {
        this.readerConfigurer = readerConfigurer;
        return this;
    }

    @Override
    public ReactiveMessageReaderBuilder<T> topic(String topicName) {
        this.topicName = topicName;
        return this;
    }

    @Override
    public ReactiveMessageReaderBuilder<T> startAtSpec(StartAtSpec startAtSpec) {
        this.startAtSpec = startAtSpec;
        return this;
    }

    @Override
    public ReactiveMessageReaderBuilder<T> endOfStreamAction(EndOfStreamAction endOfStreamAction) {
        this.endOfStreamAction = endOfStreamAction;
        return this;
    }

    @Override
    public ReactiveMessageReaderBuilder<T> clone() {
        DefaultReactiveMessageReaderBuilder<T> cloned = new DefaultReactiveMessageReaderBuilder<>(
            schema,
            reactiveReaderAdapterFactory
        );
        cloned.readerConfigurer = this.readerConfigurer;
        cloned.topicName = this.topicName;
        cloned.startAtSpec = this.startAtSpec;
        cloned.endOfStreamAction = this.endOfStreamAction;
        return this;
    }

    @Override
    public ReactiveMessageReader<T> build() {
        return new DefaultReactiveMessageReader<>(
            reactiveReaderAdapterFactory,
            schema,
            readerConfigurer,
            topicName,
            startAtSpec,
            endOfStreamAction
        );
    }
}
