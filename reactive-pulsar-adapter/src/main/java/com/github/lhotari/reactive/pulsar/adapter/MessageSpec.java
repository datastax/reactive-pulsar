package com.github.lhotari.reactive.pulsar.adapter;

import org.apache.pulsar.client.api.TypedMessageBuilder;

public interface MessageSpec<T> {

    static <T> MessageSpecBuilder<T> builder(T value) {
        return new DefaultMessageSpecBuilder<T>().value(value);
    }

    static <T> MessageSpec<T> of(T value) {
        return new ValueOnlyMessageSpec<T>(value);
    }

    void configure(TypedMessageBuilder<T> typedMessageBuilder);
}
