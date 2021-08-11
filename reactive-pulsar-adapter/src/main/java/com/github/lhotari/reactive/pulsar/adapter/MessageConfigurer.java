package com.github.lhotari.reactive.pulsar.adapter;


public interface MessageConfigurer<T> {
    void configure(MessageBuilder<T> messageBuilder);
}
