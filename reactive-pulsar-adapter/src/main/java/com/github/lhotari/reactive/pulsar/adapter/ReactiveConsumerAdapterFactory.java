package com.github.lhotari.reactive.pulsar.adapter;

import java.util.function.Function;
import org.apache.pulsar.client.api.ConsumerBuilder;
import org.apache.pulsar.client.api.PulsarClient;

public interface ReactiveConsumerAdapterFactory {
    <T> ReactiveConsumerAdapter<T> create(Function<PulsarClient, ConsumerBuilder<T>> consumerBuilderFactory);
}
