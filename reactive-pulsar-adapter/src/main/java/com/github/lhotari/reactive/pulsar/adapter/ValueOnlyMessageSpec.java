package com.github.lhotari.reactive.pulsar.adapter;

import org.apache.pulsar.client.api.TypedMessageBuilder;

class ValueOnlyMessageSpec<T> implements MessageSpec<T> {
    private final T value;

    ValueOnlyMessageSpec(T value) {
        this.value = value;
    }

    @Override
    public void configure(TypedMessageBuilder<T> typedMessageBuilder) {
        typedMessageBuilder.value(value);
    }
}
