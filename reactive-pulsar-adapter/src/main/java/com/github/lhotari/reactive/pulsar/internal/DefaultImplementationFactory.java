package com.github.lhotari.reactive.pulsar.internal;

import com.github.lhotari.reactive.pulsar.adapter.MessageResult;
import com.github.lhotari.reactive.pulsar.adapter.MessageSpec;
import com.github.lhotari.reactive.pulsar.adapter.MessageSpecBuilder;
import com.github.lhotari.reactive.pulsar.adapter.ReactiveMessageConsumer;
import com.github.lhotari.reactive.pulsar.adapter.ReactiveMessageHandlerBuilder;
import com.github.lhotari.reactive.pulsar.adapter.ReactivePulsarAdapter;
import com.github.lhotari.reactive.pulsar.adapter.ReactivePulsarClient;
import java.util.concurrent.CompletableFuture;
import java.util.function.Supplier;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.PulsarClient;
import reactor.core.publisher.Mono;

public class DefaultImplementationFactory {
    public static ReactivePulsarClient createReactivePulsarClient(ReactivePulsarAdapter reactivePulsarAdapter) {
        return new DefaultReactivePulsarClient(reactivePulsarAdapter);
    }

    public static ReactivePulsarAdapter createReactivePulsarAdapter(Supplier<PulsarClient> pulsarClientSupplier) {
        return new DefaultReactivePulsarAdapter(pulsarClientSupplier);
    }

    public static <T> ReactiveMessageHandlerBuilder<T> createReactiveMessageHandlerBuilder(ReactiveMessageConsumer<T> messageConsumer) {
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