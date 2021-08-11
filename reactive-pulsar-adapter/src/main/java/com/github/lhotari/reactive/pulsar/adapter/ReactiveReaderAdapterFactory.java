package com.github.lhotari.reactive.pulsar.adapter;

import java.util.function.Function;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.ReaderBuilder;
import org.apache.pulsar.client.api.Schema;

public interface ReactiveReaderAdapterFactory {
    <T> ReactiveReaderAdapter<T> create(Function<PulsarClient, ReaderBuilder<T>> readerBuilderFactory);
    <T> ReactiveMessageReaderFactory<T> messageReader(Schema<T> schema);
}
