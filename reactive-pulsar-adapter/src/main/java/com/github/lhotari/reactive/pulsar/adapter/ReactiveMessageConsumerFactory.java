package com.github.lhotari.reactive.pulsar.adapter;

import reactor.core.scheduler.Scheduler;

public interface ReactiveMessageConsumerFactory<T> {
    ReactiveMessageConsumerFactory<T> consumerConfigurer(ConsumerConfigurer<T> readerConfigurer);

    ReactiveMessageConsumerFactory<T> topic(String topicName);

    /**
     * When set to true, ignores the acknowledge operation completion and makes it asynchronous from the message
     * consuming processing to improve performance by allowing the acknowledges and message processing to interleave.
     * Defaults to true.
     *
     * @param acknowledgeAsynchronously When set to true, ignores the acknowledge operation completion
     * @return the current ReactiveMessageConsumerFactory instance (this)
     */
    ReactiveMessageConsumerFactory<T> acknowledgeAsynchronously(boolean acknowledgeAsynchronously);

    ReactiveMessageConsumerFactory<T> acknowledgeScheduler(Scheduler acknowledgeScheduler);

    ReactiveMessageConsumer<T> create();
}
