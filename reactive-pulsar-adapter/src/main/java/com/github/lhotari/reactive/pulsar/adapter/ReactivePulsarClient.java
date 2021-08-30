package com.github.lhotari.reactive.pulsar.adapter;

import com.github.lhotari.reactive.pulsar.internal.adapter.AdapterImplementationFactory;
import com.github.lhotari.reactive.pulsar.resourceadapter.ReactivePulsarResourceAdapter;
import java.util.function.Supplier;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.Schema;

/**
 * Adapts the Pulsar Java Client to Reactive Streams
 *
 * Contains methods to create builders for {@link ReactiveMessageSender}, {@link ReactiveMessageReader}
 * and {@link ReactiveMessageConsumer} instances.
 *
 */
public interface ReactivePulsarClient {
    /**
     * Creates a ReactivePulsarClient by wrapping an existing PulsarClient instance
     *
     * @param pulsarClient the Pulsar Client instance to use
     * @return a ReactivePulsarClient instance
     */
    static ReactivePulsarClient create(PulsarClient pulsarClient) {
        return create(() -> pulsarClient);
    }

    /**
     * Creates a ReactivePulsarClient which will lazily call the provided
     * supplier to get an instance of a Pulsar Client when needed.
     * The reference to the Pulsar client instance isn't cached. This method will be
     * called everytime when the Reactive Pulsar Adapter implementation needs access to the PulsarClient instance.
     *
     * @param pulsarClientSupplier the supplier to use for getting a Pulsar Client instance when needed
     * @return a ReactivePulsarClient instance
     */
    static ReactivePulsarClient create(Supplier<PulsarClient> pulsarClientSupplier) {
        return create(ReactivePulsarResourceAdapter.create(pulsarClientSupplier));
    }

    /**
     * Creates a ReactivePulsarClient that uses an existing ReactivePulsarResourceAdapter instance
     * for accessing Pulsar resources.
     *
     * @param reactivePulsarResourceAdapter the ReactivePulsarResourceAdapter instance
     * @return a ReactivePulsarClient instance
     */
    static ReactivePulsarClient create(ReactivePulsarResourceAdapter reactivePulsarResourceAdapter) {
        return AdapterImplementationFactory.createReactivePulsarClient(reactivePulsarResourceAdapter);
    }

    /**
     * Creates a builder for building a {@link ReactiveMessageSender}.
     *
     * @param schema the Pulsar Java client Schema for the message payload
     * @param <T> the message payload type
     * @return a builder for building a {@link ReactiveMessageSender}
     */
    <T> ReactiveMessageSenderBuilder<T> messageSender(Schema<T> schema);

    /**
     * Creates a builder for building a {@link ReactiveMessageReader}.
     *
     * @param schema the Pulsar Java client Schema for the message payload
     * @param <T> the message payload type
     * @return a builder for building a {@link ReactiveMessageReader}
     */
    <T> ReactiveMessageReaderBuilder<T> messageReader(Schema<T> schema);

    /**
     * Creates a builder for building a {@link ReactiveMessageConsumer}.
     *
     * @param schema the Pulsar Java client Schema for the message payload
     * @param <T> the message payload type
     * @return a builder for building a {@link ReactiveMessageConsumer}
     */
    <T> ReactiveMessageConsumerBuilder<T> messageConsumer(Schema<T> schema);
}
