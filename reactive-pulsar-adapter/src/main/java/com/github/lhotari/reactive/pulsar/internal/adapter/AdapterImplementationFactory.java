package com.github.lhotari.reactive.pulsar.internal.adapter;

import com.github.lhotari.reactive.pulsar.adapter.*;
import com.github.lhotari.reactive.pulsar.resourceadapter.ReactivePulsarResourceAdapter;
import java.util.concurrent.CompletableFuture;
import java.util.function.Supplier;
import org.apache.pulsar.client.api.MessageId;
import reactor.core.publisher.Mono;

public class AdapterImplementationFactory {

    public static ReactivePulsarClient createReactivePulsarClient(
        ReactivePulsarResourceAdapter reactivePulsarResourceAdapter
    ) {
        return new DefaultReactivePulsarClient(reactivePulsarResourceAdapter);
    }

    public static <T> ReactiveMessageHandlerBuilder<T> createReactiveMessageHandlerBuilder(
        ReactiveMessageConsumer<T> messageConsumer
    ) {
        return new DefaultReactiveMessageHandlerBuilder<>(messageConsumer);
    }

    public static <T> MessageResult<T> acknowledge(MessageId messageId, T value) {
        return new DefaultMessageResult<>(messageId, true, value);
    }

    public static <T> MessageResult<T> negativeAcknowledge(MessageId messageId, T value) {
        return new DefaultMessageResult<T>(messageId, false, value);
    }

    public static MessageResult<Void> acknowledge(MessageId messageId) {
        return new EmptyMessageResult(messageId, true);
    }

    public static MessageResult<Void> negativeAcknowledge(MessageId messageId) {
        return new EmptyMessageResult(messageId, false);
    }

    public static <T> MessageSpecBuilder<T> createMessageSpecBuilder(T value) {
        return new DefaultMessageSpecBuilder<T>().value(value);
    }

    public static <T> MessageSpec<T> createValueOnlyMessageSpec(T value) {
        return new ValueOnlyMessageSpec<>(value);
    }

    public static <T> Mono<T> adaptPulsarFuture(Supplier<? extends CompletableFuture<T>> futureSupplier) {
        return PulsarFutureAdapter.adaptPulsarFuture(futureSupplier);
    }
}
