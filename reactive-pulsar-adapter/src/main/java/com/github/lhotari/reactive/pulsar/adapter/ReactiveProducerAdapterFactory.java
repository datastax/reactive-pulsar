package com.github.lhotari.reactive.pulsar.adapter;

import java.util.function.Function;
import org.apache.pulsar.client.api.ProducerBuilder;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.Schema;

public interface ReactiveProducerAdapterFactory {
    ReactiveProducerAdapterFactory cache(ReactiveProducerCache producerCache);
    <T> ReactiveProducerAdapter<T> create(Function<PulsarClient, ProducerBuilder<T>> producerBuilderFactory);
    <T> ReactiveMessageSenderFactory<T> messageSender(Schema<T> schema);
}