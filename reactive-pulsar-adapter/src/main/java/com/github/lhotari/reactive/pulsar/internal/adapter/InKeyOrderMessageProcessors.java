package com.github.lhotari.reactive.pulsar.internal.adapter;

import java.util.function.Function;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.impl.Murmur3_32Hash;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Flux;
import reactor.core.publisher.GroupedFlux;
import reactor.core.scheduler.Scheduler;

/**
 * Functions for implementing In-order parallel processing for Pulsar messages using Project Reactor.
 *
 * A processing group is resolved for each message based on the message's key.
 * The message flux is split into group fluxes based on the processing group.
 * Each group flux is processes messages in order (one-by-one). Multiple group fluxes are processed in parallel.
 */
public class InKeyOrderMessageProcessors {

    /**
     *  Resolves a processing group for a message based on the key information.
     *
     *  Uses Pulsar's Murmur3_32Hash function to calculate a hash of the key information.
     *
     * @param message the Pulsar message
     * @param numberOfGroups maximum number of groups
     * @return processing group for the message, in the range of 0 to numberOfGroups, exclusive
     */
    public static int resolveProcessingGroupForMessage(Message<?> message, int numberOfGroups) {
        byte[] keyBytes = getMessageKeyBytes(message);
        int keyHash = Murmur3_32Hash.getInstance().makeHash(keyBytes);
        return keyHash % numberOfGroups;
    }

    private static byte[] getMessageKeyBytes(Message<?> message) {
        byte[] keyBytes = null;
        if (message.hasOrderingKey()) {
            keyBytes = message.getOrderingKey();
        } else if (message.hasKey()) {
            keyBytes = message.getKeyBytes();
        }
        if (keyBytes == null || keyBytes.length == 0) {
            // use a group that has been derived from the message id so that redeliveries get handled in order
            keyBytes = message.getMessageId().toByteArray();
        }
        return keyBytes;
    }

    /**
     * Splits the flux of messages by message key into the given number of groups.
     *
     * @param messageFlux flux of messages
     * @param numberOfGroups number of processing groups
     * @param <T> message payload type
     * @return the grouped flux of messages
     */
    public static <T> Flux<GroupedFlux<Integer, Message<T>>> groupByProcessingGroup(
        Flux<Message<T>> messageFlux,
        int numberOfGroups
    ) {
        return messageFlux.groupBy(message -> resolveProcessingGroupForMessage(message, numberOfGroups));
    }

    /**
     * Processes the messages concurrently with the targeted concurrency. Uses ".flatMap" in the implementation
     *
     * @param messageFlux the flux of messages
     * @param messageHandler message handler function
     * @param scheduler scheduler to use for subscribing to inner publishers
     * @param concurrency targeted concurrency level
     * @param <T> message payload type
     * @param <R> message handler's resulting type
     * @return flux of message handler results
     */
    public static <T, R> Flux<R> processInKeyOrderConcurrently(
        Flux<Message<T>> messageFlux,
        Function<? super Message<T>, ? extends Publisher<? extends R>> messageHandler,
        Scheduler scheduler,
        int concurrency
    ) {
        return groupByProcessingGroup(messageFlux, concurrency)
            .flatMap(groupedFlux -> groupedFlux.concatMap(messageHandler).subscribeOn(scheduler), concurrency);
    }

    /**
     * Processes the messages in parallel with the targeted parallelism. Uses ".parallel" in the implementation
     *
     * @param messageFlux the flux of messages
     * @param messageHandler message handler function
     * @param scheduler scheduler to use for subscribing to inner publishers
     * @param parallelism targeted level of parallelism
     * @param <T> message payload type
     * @param <R> message handler's resulting type
     * @return flux of message handler results
     */
    public static <T, R> Flux<R> processInKeyOrderInParallel(
        Flux<Message<T>> messageFlux,
        Function<? super Message<T>, ? extends Publisher<? extends R>> messageHandler,
        Scheduler scheduler,
        int parallelism
    ) {
        return groupByProcessingGroup(messageFlux, parallelism)
            .parallel(parallelism)
            .runOn(scheduler)
            .flatMap(groupedFlux -> groupedFlux.concatMap(messageHandler))
            .sequential();
    }
}
