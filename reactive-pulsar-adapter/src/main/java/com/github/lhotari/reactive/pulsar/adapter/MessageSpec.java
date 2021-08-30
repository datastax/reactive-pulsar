package com.github.lhotari.reactive.pulsar.adapter;

import com.github.lhotari.reactive.pulsar.internal.adapter.AdapterImplementationFactory;
import org.apache.pulsar.client.api.TypedMessageBuilder;

public interface MessageSpec<T> {
    static <T> MessageSpecBuilder<T> builder(T value) {
        return AdapterImplementationFactory.createMessageSpecBuilder(value);
    }

    static <T> MessageSpec<T> of(T value) {
        return AdapterImplementationFactory.createValueOnlyMessageSpec(value);
    }

    void configure(TypedMessageBuilder<T> typedMessageBuilder);
}
