package com.github.lhotari.reactive.pulsar.adapter;

import static org.assertj.core.api.Assertions.assertThat;

import com.github.lhotari.reactive.pulsar.producercache.CaffeineReactiveProducerCache;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import org.apache.pulsar.client.api.*;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Mono;

public class ReactiveMessageSenderE2ETest {

    @Test
    void shouldSendMessageToTopic() throws PulsarClientException {
        try (PulsarClient pulsarClient = SingletonPulsarContainer.createPulsarClient()) {
            String topicName = "test" + UUID.randomUUID();
            Consumer<String> consumer = pulsarClient
                .newConsumer(Schema.STRING)
                .topic(topicName)
                .subscriptionName("sub")
                .subscribe();

            ReactivePulsarClient reactivePulsarClient = ReactivePulsarClient.create(pulsarClient);

            ReactiveMessageSender<String> messageSender = reactivePulsarClient
                .messageSender(Schema.STRING)
                .topic(topicName)
                .maxInflight(1)
                .build();
            MessageId messageId = messageSender.sendMessage(Mono.just(MessageSpec.of("Hello world!"))).block();
            assertThat(messageId).isNotNull();

            Message<String> message = consumer.receive(1, TimeUnit.SECONDS);
            assertThat(message).isNotNull();
            assertThat(message.getValue()).isEqualTo("Hello world!");
        }
    }

    @Test
    void shouldSendMessageToTopicWithCachedProducer() throws PulsarClientException {
        try (
            PulsarClient pulsarClient = SingletonPulsarContainer.createPulsarClient();
            CaffeineReactiveProducerCache producerCache = new CaffeineReactiveProducerCache()
        ) {
            String topicName = "test" + UUID.randomUUID();
            Consumer<String> consumer = pulsarClient
                .newConsumer(Schema.STRING)
                .topic(topicName)
                .subscriptionName("sub")
                .subscribe();

            ReactivePulsarClient reactivePulsarClient = ReactivePulsarClient.create(pulsarClient);

            ReactiveMessageSender<String> messageSender = reactivePulsarClient
                .messageSender(Schema.STRING)
                .cache(producerCache)
                .maxInflight(1)
                .topic(topicName)
                .build();
            MessageId messageId = messageSender.sendMessage(Mono.just(MessageSpec.of("Hello world!"))).block();
            assertThat(messageId).isNotNull();

            Message<String> message = consumer.receive(1, TimeUnit.SECONDS);
            assertThat(message).isNotNull();
            assertThat(message.getValue()).isEqualTo("Hello world!");
        }
    }
}
