package com.github.lhotari.reactive.pulsar.spring.app;

import static org.assertj.core.api.Assertions.assertThat;

import com.github.lhotari.reactive.pulsar.adapter.MessageSpec;
import com.github.lhotari.reactive.pulsar.adapter.ReactiveMessageSender;
import com.github.lhotari.reactive.pulsar.adapter.ReactivePulsarClient;
import com.github.lhotari.reactive.pulsar.resourcewrapper.ReactiveProducerCache;
import com.github.lhotari.reactive.pulsar.spring.test.SingletonPulsarContainer;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import org.apache.pulsar.client.api.*;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.ContextConfiguration;
import reactor.core.publisher.Mono;

@SpringBootTest
@ContextConfiguration(initializers = SingletonPulsarContainer.ContextInitializer.class)
public class ReactiveMessageSenderE2ETest {

    @Autowired
    PulsarClient pulsarClient;

    @Autowired
    ReactivePulsarClient reactivePulsarClient;

    @Autowired
    ReactiveProducerCache reactiveProducerCache;

    @Test
    void shouldSendMessageToTopic() throws PulsarClientException {
        String topicName = "test" + UUID.randomUUID();
        Consumer<String> consumer = pulsarClient
            .newConsumer(Schema.STRING)
            .topic(topicName)
            .subscriptionName("sub")
            .subscribe();

        ReactiveMessageSender<String> messageSender = reactivePulsarClient
            .messageSender(Schema.STRING)
            .topic(topicName)
            .build();
        MessageId messageId = messageSender.sendMessage(Mono.just(MessageSpec.of("Hello world!"))).block();
        assertThat(messageId).isNotNull();

        Message<String> message = consumer.receive(1, TimeUnit.SECONDS);
        assertThat(message).isNotNull();
        assertThat(message.getValue()).isEqualTo("Hello world!");
    }

    @Test
    void shouldSendMessageToTopicWithCachedProducer() throws PulsarClientException {
        String topicName = "test" + UUID.randomUUID();
        Consumer<String> consumer = pulsarClient
            .newConsumer(Schema.STRING)
            .topic(topicName)
            .subscriptionName("sub")
            .subscribe();

        ReactiveMessageSender<String> messageSender = reactivePulsarClient
            .messageSender(Schema.STRING)
            .cache(reactiveProducerCache)
            .topic(topicName)
            .build();
        MessageId messageId = messageSender.sendMessage(Mono.just(MessageSpec.of("Hello world!"))).block();
        assertThat(messageId).isNotNull();

        Message<String> message = consumer.receive(1, TimeUnit.SECONDS);
        assertThat(message).isNotNull();
        assertThat(message.getValue()).isEqualTo("Hello world!");
    }
}
