package com.github.lhotari.reactive.pulsar.adapter;

import java.util.concurrent.atomic.AtomicReference;
import reactor.core.Disposable;
import reactor.core.publisher.Mono;

class DefaultReactiveMessageHandler implements ReactiveMessageHandler {
    private final AtomicReference<Disposable> killSwitch = new AtomicReference<>();
    private final Mono<Void> pipeline;

    public DefaultReactiveMessageHandler(Mono<Void> pipeline) {
        this.pipeline = pipeline;
    }

    @Override
    public ReactiveMessageHandler start() {
        if (killSwitch.get() != null) {
            throw new IllegalStateException("Message handler is already running.");
        }
        Disposable disposable = pipeline.subscribe();
        if (!killSwitch.compareAndSet(null, disposable)) {
            disposable.dispose();
            throw new IllegalStateException("Message handler was already running.");
        }
        return this;
    }

    @Override
    public ReactiveMessageHandler stop() {
        Disposable disposable = killSwitch.getAndSet(null);
        if (disposable != null) {
            disposable.dispose();
        }
        return this;
    }

    @Override
    public boolean isRunning() {
        return killSwitch.get() != null;
    }

    @Override
    public void close() throws Exception {
        stop();
    }
}

