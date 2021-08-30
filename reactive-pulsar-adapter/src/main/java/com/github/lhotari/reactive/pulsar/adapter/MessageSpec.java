package com.github.lhotari.reactive.pulsar.adapter;

import com.github.lhotari.reactive.pulsar.internal.DefaultImplementationFactory;
import org.apache.pulsar.client.api.TypedMessageBuilder;

public interface MessageSpec<T> {
    static <T> MessageSpecBuilder<T> builder(T value) {
        return DefaultImplementationFactory.createMessageSpecBuilder(value);
    }

    static <T> MessageSpec<T> of(T value) {
        return DefaultImplementationFactory.createValueOnlyMessageSpec(value);
    }

    void configure(TypedMessageBuilder<T> typedMessageBuilder);
}
