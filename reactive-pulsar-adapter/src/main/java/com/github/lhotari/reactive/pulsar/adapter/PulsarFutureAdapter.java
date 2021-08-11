package com.github.lhotari.reactive.pulsar.adapter;

import java.util.concurrent.CompletableFuture;
import java.util.function.Supplier;
import org.apache.pulsar.client.api.PulsarClientException;
import reactor.core.publisher.Mono;
import reactor.core.publisher.SignalType;

/**
 * Stateful adapter from CompletableFuture to Mono which keeps a reference to
 * the original future so that it can be cancelled. Cancellation is necessary
 * for some cases to release resources.
 *
 * There's additional logic to ignore Pulsar client's
 * {@link org.apache.pulsar.client.api.PulsarClientException.AlreadyClosedException}
 * when the Mono has been cancelled. This is to reduce unnecessary exceptions in logs.
 */
public class PulsarFutureAdapter {
    private volatile boolean cancelled;
    private volatile CompletableFuture<?> futureReference;

    public static <T> Mono<T> adaptPulsarFuture(Supplier<? extends CompletableFuture<T>> futureSupplier) {
        return Mono.defer(() -> new PulsarFutureAdapter().toMono(futureSupplier));
    }

    private PulsarFutureAdapter() {

    }

    <T> Mono<? extends T> toMono(Supplier<? extends CompletableFuture<T>> futureSupplier) {
        return Mono.fromFuture(() -> createFuture(futureSupplier))
                .doOnCancel(this::doOnCancel)
                .doFinally(this::doFinally);
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
    }

    private void doFinally(SignalType signal) {
        if (signal == SignalType.CANCEL) {
            CompletableFuture<?> future = futureReference;
            if (future != null) {
                future.cancel(false);
            }
        }
        futureReference = null;
    }

    private static void handleException(boolean cancelled, Throwable e) {
        if (cancelled) {
            rethrowIfNotAlreadyClosedException(e);
        } else {
            sneakyThrow(e);
        }
    }

    private static void rethrowIfNotAlreadyClosedException(Throwable e) {
        if (!isAlreadyClosedCause(e)) {
            sneakyThrow(e);
        }
    }

    private static boolean isAlreadyClosedCause(Throwable e) {
        return e instanceof PulsarClientException.AlreadyClosedException ||
                e.getCause() instanceof PulsarClientException.AlreadyClosedException;
    }

    private static <E extends Throwable> void sneakyThrow(Throwable e) throws E {
        throw (E) e;
    }
}
