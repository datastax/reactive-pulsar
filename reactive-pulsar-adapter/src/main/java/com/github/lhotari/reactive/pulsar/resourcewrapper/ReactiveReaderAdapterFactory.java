package com.github.lhotari.reactive.pulsar.resourcewrapper;

import java.util.function.Function;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.ReaderBuilder;

public interface ReactiveReaderAdapterFactory {
    <T> ReactiveReaderAdapter<T> create(Function<PulsarClient, ReaderBuilder<T>> readerBuilderFactory);
}
