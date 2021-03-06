package com.github.lhotari.reactive.pulsar.internal.adapter;

import com.github.lhotari.reactive.pulsar.adapter.ConsumerConfigurer;
import com.github.lhotari.reactive.pulsar.adapter.ReactiveMessageConsumer;
import com.github.lhotari.reactive.pulsar.adapter.ReactiveMessageConsumerBuilder;
import com.github.lhotari.reactive.pulsar.resourceadapter.ReactiveConsumerAdapterFactory;
import org.apache.pulsar.client.api.Schema;
import reactor.core.scheduler.Scheduler;
import reactor.core.scheduler.Schedulers;

class DefaultReactiveMessageConsumerBuilder<T> implements ReactiveMessageConsumerBuilder<T> {

    private final Schema<T> schema;
    private final ReactiveConsumerAdapterFactory reactiveConsumerAdapterFactory;
    private ConsumerConfigurer<T> readerConfigurer;
    private String topicName;
    private boolean acknowledgeAsynchronously = true;
    private Scheduler acknowledgeScheduler = Schedulers.boundedElastic();

    public DefaultReactiveMessageConsumerBuilder(
        Schema<T> schema,
        ReactiveConsumerAdapterFactory reactiveConsumerAdapterFactory
    ) {
        this.schema = schema;
        this.reactiveConsumerAdapterFactory = reactiveConsumerAdapterFactory;
    }

    @Override
    public ReactiveMessageConsumerBuilder<T> consumerConfigurer(ConsumerConfigurer<T> readerConfigurer) {
        this.readerConfigurer = readerConfigurer;
        return this;
    }

    @Override
    public ReactiveMessageConsumerBuilder<T> topic(String topicName) {
        this.topicName = topicName;
        return this;
    }

    @Override
    public ReactiveMessageConsumerBuilder<T> acknowledgeAsynchronously(boolean acknowledgeAsynchronously) {
        this.acknowledgeAsynchronously = acknowledgeAsynchronously;
        return this;
    }

    public ReactiveMessageConsumerBuilder<T> acknowledgeScheduler(Scheduler acknowledgeScheduler) {
        this.acknowledgeScheduler = acknowledgeScheduler;
        return this;
    }

    @Override
    public ReactiveMessageConsumer<T> build() {
        return new DefaultReactiveMessageConsumer<T>(
            reactiveConsumerAdapterFactory,
            schema,
            readerConfigurer,
            topicName,
            acknowledgeAsynchronously,
            acknowledgeScheduler
        );
    }
}
