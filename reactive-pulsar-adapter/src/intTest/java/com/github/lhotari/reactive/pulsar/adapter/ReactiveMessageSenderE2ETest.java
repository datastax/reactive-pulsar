package com.github.lhotari.reactive.pulsar.adapter;

import static org.assertj.core.api.Assertions.assertThat;
import com.github.lhotari.reactive.pulsar.producercache.CaffeineReactiveProducerCache;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.Schema;
import org.junit.jupiter.api.Test;

public class ReactiveMessageSenderE2ETest {

    @Test
    void shouldSendMessageToTopic() throws PulsarClientException {
        try (PulsarClient pulsarClient = SingletonPulsarContainer.createPulsarClient()) {
            String topicName = "test" + UUID.randomUUID();
            Consumer<String> consumer = pulsarClient.newConsumer(Schema.STRING)
                    .topic(topicName)
                    .subscriptionName("sub")
                    .subscribe();

            ReactiveMessageSender<String> messageSender = ReactivePulsarAdapter.create(pulsarClient)
                    .producer()
                    .messageSender(Schema.STRING)
                    .topic(topicName)
                    .create();
            MessageId messageId = messageSender.sendMessagePayload("Hello world!")
                    .block();
            assertThat(messageId).isNotNull();

            Message<String> message = consumer.receive(1, TimeUnit.SECONDS);
            assertThat(message).isNotNull();
            assertThat(message.getValue()).isEqualTo("Hello world!");
        }
    }

    @Test
    void shouldSendMessageToTopicWithCachedProducer() throws PulsarClientException {
        try (PulsarClient pulsarClient = SingletonPulsarContainer.createPulsarClient();
             CaffeineReactiveProducerCache producerCache = new CaffeineReactiveProducerCache()) {
            String topicName = "test" + UUID.randomUUID();
            Consumer<String> consumer = pulsarClient.newConsumer(Schema.STRING)
                    .topic(topicName)
                    .subscriptionName("sub")
                    .subscribe();

            ReactiveMessageSender<String> messageSender = ReactivePulsarAdapter.create(pulsarClient)
                    .producer()
                    .cache(producerCache)
                    .messageSender(Schema.STRING)
                    .topic(topicName)
                    .create();
            MessageId messageId = messageSender.sendMessagePayload("Hello world!")
                    .block();
            assertThat(messageId).isNotNull();

            Message<String> message = consumer.receive(1, TimeUnit.SECONDS);
            assertThat(message).isNotNull();
            assertThat(message.getValue()).isEqualTo("Hello world!");
        }
    }
}