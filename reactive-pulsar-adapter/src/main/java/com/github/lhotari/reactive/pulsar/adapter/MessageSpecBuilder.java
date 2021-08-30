package com.github.lhotari.reactive.pulsar.adapter;

import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.SubscriptionType;

public interface MessageSpecBuilder<T> {
    /**
     * Sets the key of the message for routing policy.
     *
     * @param key the partitioning key for the message
     * @return the message builder instance
     */
    MessageSpecBuilder<T> key(String key);

    /**
     * Sets the bytes of the key of the message for routing policy.
     * Internally the bytes will be base64 encoded.
     *
     * @param key routing key for message, in byte array form
     * @return the message builder instance
     */
    MessageSpecBuilder<T> keyBytes(byte[] key);

    /**
     * Sets the ordering key of the message for message dispatch in {@link SubscriptionType#Key_Shared} mode.
     * Partition key Will be used if ordering key not specified.
     *
     * @param orderingKey the ordering key for the message
     * @return the message builder instance
     */
    MessageSpecBuilder<T> orderingKey(byte[] orderingKey);

    /**
     * Set a domain object on the message.
     *
     * @param value the domain object
     * @return the message builder instance
     */
    MessageSpecBuilder<T> value(T value);

    /**
     * Sets a new property on a message.
     *
     * @param name  the name of the property
     * @param value the associated value
     * @return the message builder instance
     */
    MessageSpecBuilder<T> property(String name, String value);

    /**
     * Add all the properties in the provided map.
     *
     * @return the message builder instance
     */
    MessageSpecBuilder<T> properties(Map<String, String> properties);

    /**
     * Set the event time for a given message.
     *
     * <p>Applications can retrieve the event time by calling {@link Message#getEventTime()}.
     *
     * <p>Note: currently pulsar doesn't support event-time based index. so the subscribers
     * can't seek the messages by event time.
     *
     * @return the message builder instance
     */
    MessageSpecBuilder<T> eventTime(long timestamp);

    /**
     * Specify a custom sequence id for the message being published.
     *
     * <p>The sequence id can be used for deduplication purposes and it needs to follow these rules:
     * <ol>
     * <li><code>sequenceId &gt;= 0</code>
     * <li>Sequence id for a message needs to be greater than sequence id for earlier messages:
     * <code>sequenceId(N+1) &gt; sequenceId(N)</code>
     * <li>It's not necessary for sequence ids to be consecutive. There can be holes between messages. Eg. the
     * <code>sequenceId</code> could represent an offset or a cumulative size.
     * </ol>
     *
     * @param sequenceId the sequence id to assign to the current message
     * @return the message builder instance
     */
    MessageSpecBuilder<T> sequenceId(long sequenceId);

    /**
     * Override the geo-replication clusters for this message.
     *
     * @param clusters the list of clusters.
     * @return the message builder instance
     */
    MessageSpecBuilder<T> replicationClusters(List<String> clusters);

    /**
     * Disable geo-replication for this message.
     *
     * @return the message builder instance
     */
    MessageSpecBuilder<T> disableReplication();

    /**
     * Deliver the message only at or after the specified absolute timestamp.
     *
     * <p>The timestamp is milliseconds and based on UTC (eg: {@link System#currentTimeMillis()}.
     *
     * <p><b>Note</b>: messages are only delivered with delay when a consumer is consuming
     * through a {@link SubscriptionType#Shared} subscription. With other subscription
     * types, the messages will still be delivered immediately.
     *
     * @param timestamp absolute timestamp indicating when the message should be delivered to consumers
     * @return the message builder instance
     */
    MessageSpecBuilder<T> deliverAt(long timestamp);

    /**
     * Request to deliver the message only after the specified relative delay.
     *
     * <p><b>Note</b>: messages are only delivered with delay when a consumer is consuming
     * through a {@link SubscriptionType#Shared} subscription. With other subscription
     * types, the messages will still be delivered immediately.
     *
     * @param delay the amount of delay before the message will be delivered
     * @param unit  the time unit for the delay
     * @return the message builder instance
     */
    MessageSpecBuilder<T> deliverAfter(long delay, TimeUnit unit);

    MessageSpec<T> build();
}
