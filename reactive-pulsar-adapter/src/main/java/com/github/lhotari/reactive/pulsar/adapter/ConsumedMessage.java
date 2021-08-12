package com.github.lhotari.reactive.pulsar.adapter;

import org.apache.pulsar.client.api.Message;

public interface ConsumedMessage<T> {
    Message<T> getMessage();
    void acknowledge();
    void negativeAcknowledge();
}
