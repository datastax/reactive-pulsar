package com.github.lhotari.reactive.pulsar.adapter;

import org.apache.pulsar.client.api.TypedMessageBuilder;

public interface MessageSpec<T> {
    static <T> MessageSpecBuilder<T> builder() {
        return new DefaultMessageSpecBuilder<>();
    }
    void configure(TypedMessageBuilder<T> typedMessageBuilder);
}
