package com.github.lhotari.reactive.pulsar.internal;

import com.github.lhotari.reactive.pulsar.adapter.ConsumerConfigurer;
import com.github.lhotari.reactive.pulsar.adapter.ReactiveConsumerAdapterFactory;
import com.github.lhotari.reactive.pulsar.adapter.ReactiveMessageConsumer;
import com.github.lhotari.reactive.pulsar.adapter.ReactiveMessageConsumerFactory;
import org.apache.pulsar.client.api.Schema;
import reactor.core.scheduler.Scheduler;
import reactor.core.scheduler.Schedulers;

class DefaultReactiveMessageConsumerFactory<T> implements ReactiveMessageConsumerFactory<T> {
    private final Schema<T> schema;
    private final ReactiveConsumerAdapterFactory reactiveConsumerAdapterFactory;
    private ConsumerConfigurer<T> readerConfigurer;
    private String topicName;
    private boolean acknowledgeAsynchronously = true;
    private Scheduler acknowledgeScheduler = Schedulers.boundedElastic();

    public DefaultReactiveMessageConsumerFactory(Schema<T> schema,
                                                 ReactiveConsumerAdapterFactory reactiveConsumerAdapterFactory) {
        this.schema = schema;
        this.reactiveConsumerAdapterFactory = reactiveConsumerAdapterFactory;
    }

    @Override
    public ReactiveMessageConsumerFactory<T> consumerConfigurer(ConsumerConfigurer<T> readerConfigurer) {
        this.readerConfigurer = readerConfigurer;
        return this;
    }

    @Override
    public ReactiveMessageConsumerFactory<T> topic(String topicName) {
        this.topicName = topicName;
        return this;
    }

    @Override
    public ReactiveMessageConsumerFactory<T> acknowledgeAsynchronously(boolean acknowledgeAsynchronously) {
        this.acknowledgeAsynchronously = acknowledgeAsynchronously;
        return this;
    }

    public ReactiveMessageConsumerFactory<T> acknowledgeScheduler(
            Scheduler acknowledgeScheduler) {
        this.acknowledgeScheduler = acknowledgeScheduler;
        return this;
    }

    @Override
    public ReactiveMessageConsumer<T> create() {
        return new DefaultReactiveMessageConsumer<T>(reactiveConsumerAdapterFactory, schema, readerConfigurer,
                topicName, acknowledgeAsynchronously, acknowledgeScheduler);
    }
}
