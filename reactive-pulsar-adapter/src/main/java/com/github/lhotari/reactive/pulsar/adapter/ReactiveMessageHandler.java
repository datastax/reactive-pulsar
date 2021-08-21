package com.github.lhotari.reactive.pulsar.adapter;

public interface ReactiveMessageHandler extends AutoCloseable {
    ReactiveMessageHandler start();

    ReactiveMessageHandler stop();

    boolean isRunning();
}
