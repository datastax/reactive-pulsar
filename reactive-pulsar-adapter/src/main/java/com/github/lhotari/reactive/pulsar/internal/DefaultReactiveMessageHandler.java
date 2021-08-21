package com.github.lhotari.reactive.pulsar.internal;

import com.github.lhotari.reactive.pulsar.adapter.ReactiveMessageHandler;
import java.util.concurrent.atomic.AtomicReference;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.Disposable;
import reactor.core.publisher.Mono;

class DefaultReactiveMessageHandler implements ReactiveMessageHandler {
    private final Logger LOG = LoggerFactory.getLogger(DefaultReactiveMessageHandler.class);
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
        Disposable disposable = pipeline.subscribe(null, this::logError, this::logUnexpectedCompletion);
        if (!killSwitch.compareAndSet(null, disposable)) {
            disposable.dispose();
            throw new IllegalStateException("Message handler was already running.");
        }
        return this;
    }

    private void logError(Throwable throwable) {
        LOG.error("ReactiveMessageHandler was unexpectedly terminated.", throwable);
    }

    private void logUnexpectedCompletion() {
        if (isRunning()) {
            LOG.error("ReactiveMessageHandler was unexpectedly completed.");
        }
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

