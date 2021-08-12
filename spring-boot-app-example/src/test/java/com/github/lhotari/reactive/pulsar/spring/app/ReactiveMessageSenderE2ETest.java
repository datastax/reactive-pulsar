package com.github.lhotari.reactive.pulsar.spring.app;

import static org.assertj.core.api.Assertions.assertThat;
import com.github.lhotari.reactive.pulsar.adapter.MessageSpec;
import com.github.lhotari.reactive.pulsar.adapter.ReactiveMessageSender;
import com.github.lhotari.reactive.pulsar.adapter.ReactiveProducerCache;
import com.github.lhotari.reactive.pulsar.adapter.ReactivePulsarAdapter;
import com.github.lhotari.reactive.pulsar.adapter.ReactivePulsarClient;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.Schema;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import reactor.core.publisher.Mono;

@SpringBootTest
public class ReactiveMessageSenderE2ETest {
    @DynamicPropertySource
    static void registerPulsarProperties(DynamicPropertyRegistry registry) {
        SingletonPulsarContainer.registerPulsarProperties(registry);
    }

    @Autowired
    PulsarClient pulsarClient;

    @Autowired
    ReactivePulsarAdapter reactivePulsarAdapter;

    @Autowired
    ReactiveProducerCache reactiveProducerCache;

    @Test
    void shouldSendMessageToTopic() throws PulsarClientException {
        String topicName = "test" + UUID.randomUUID();
        Consumer<String> consumer = pulsarClient.newConsumer(Schema.STRING)
                .topic(topicName)
                .subscriptionName("sub")
                .subscribe();

        ReactivePulsarClient reactivePulsarClient = ReactivePulsarClient.create(pulsarClient);

        ReactiveMessageSender<String> messageSender = reactivePulsarClient
                .messageSender(Schema.STRING)
                .topic(topicName)
                .create();
        MessageId messageId = messageSender
                .sendMessage(Mono.just(MessageSpec.<String>builder().value("Hello world!").build()))
                .block();
        assertThat(messageId).isNotNull();

        Message<String> message = consumer.receive(1, TimeUnit.SECONDS);
        assertThat(message).isNotNull();
        assertThat(message.getValue()).isEqualTo("Hello world!");
    }

    @Test
    void shouldSendMessageToTopicWithCachedProducer() throws PulsarClientException {
        String topicName = "test" + UUID.randomUUID();
        Consumer<String> consumer = pulsarClient.newConsumer(Schema.STRING)
                .topic(topicName)
                .subscriptionName("sub")
                .subscribe();

        ReactivePulsarClient reactivePulsarClient = ReactivePulsarClient.create(pulsarClient);

        ReactiveMessageSender<String> messageSender = reactivePulsarClient
                .messageSender(Schema.STRING)
                .cache(reactiveProducerCache)
                .topic(topicName)
                .create();
        MessageId messageId = messageSender
                .sendMessage(Mono.just(MessageSpec.<String>builder().value("Hello world!").build()))
                .block();
        assertThat(messageId).isNotNull();

        Message<String> message = consumer.receive(1, TimeUnit.SECONDS);
        assertThat(message).isNotNull();
        assertThat(message.getValue()).isEqualTo("Hello world!");
    }
}
