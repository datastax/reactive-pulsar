package com.github.lhotari.reactive.pulsar.spring;

import com.github.lhotari.reactive.pulsar.adapter.ReactiveMessageHandler;
import org.springframework.context.SmartLifecycle;

public abstract class AbstractReactiveMessageListenerContainer implements SmartLifecycle {
    private ReactiveMessageHandler reactiveMessageHandler;

    protected abstract ReactiveMessageHandler createReactiveMessageHandler();

    @Override
    public synchronized void start() {
        if (reactiveMessageHandler == null) {
            reactiveMessageHandler = createReactiveMessageHandler();
        }
        reactiveMessageHandler.start();
    }

    @Override
    public synchronized void stop() {
        if (reactiveMessageHandler != null) {
            reactiveMessageHandler.stop();
        }
    }

    @Override
    public synchronized boolean isRunning() {
        return reactiveMessageHandler != null ? reactiveMessageHandler.isRunning() : false;
    }
}
