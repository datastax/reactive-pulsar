package com.github.lhotari.reactive.pulsar.internal.adapter;

import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.function.Supplier;
import org.apache.pulsar.client.api.PulsarClientException;
import reactor.core.publisher.Mono;

/**
 * Stateful adapter from CompletableFuture to Mono which keeps a reference to
 * the original future so that it can be cancelled. Cancellation is necessary
 * for some cases to release resources.
 *
 * There's additional logic to ignore Pulsar client's
 * {@link org.apache.pulsar.client.api.PulsarClientException.AlreadyClosedException}
 * when the Mono has been cancelled. This is to reduce unnecessary exceptions in logs.
 */
class PulsarFutureAdapter {

    private volatile boolean cancelled;
    private volatile CompletableFuture<?> futureReference;

    static <T> Mono<T> adaptPulsarFuture(Supplier<? extends CompletableFuture<T>> futureSupplier) {
        return Mono.defer(() -> new PulsarFutureAdapter().toMono(futureSupplier));
    }

    private PulsarFutureAdapter() {}

    <T> Mono<? extends T> toMono(Supplier<? extends CompletableFuture<T>> futureSupplier) {
        return Mono.fromFuture(() -> createFuture(futureSupplier)).doOnCancel(this::doOnCancel);
    }

    private <T> CompletableFuture<T> createFuture(Supplier<? extends CompletableFuture<T>> futureSupplier) {
        try {
            CompletableFuture<T> future = futureSupplier.get();
            futureReference = future;
            return future.exceptionally(e -> {
                handleException(cancelled, e);
                return null;
            });
        } catch (Exception e) {
            handleException(cancelled, e);
            return CompletableFuture.completedFuture(null);
        }
    }

    private void doOnCancel() {
        cancelled = true;
        CompletableFuture<?> future = futureReference;
        if (future != null) {
            future.cancel(false);
        }
    }

    private static void handleException(boolean cancelled, Throwable e) {
        if (cancelled) {
            rethrowIfRelevantException(e);
        } else {
            sneakyThrow(e);
        }
    }

    private static boolean isAlreadyClosedCause(Throwable e) {
        return (
            e instanceof PulsarClientException.AlreadyClosedException ||
            e.getCause() instanceof PulsarClientException.AlreadyClosedException
        );
    }

    private static void rethrowIfRelevantException(Throwable e) {
        if (!isAlreadyClosedCause(e) && !(e instanceof CancellationException)) {
            sneakyThrow(e);
        }
    }

    private static <E extends Throwable> void sneakyThrow(Throwable e) throws E {
        throw (E) e;
    }
}
