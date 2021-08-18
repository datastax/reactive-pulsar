package com.github.lhotari.reactive.pulsar.adapter;

import static org.assertj.core.api.Assertions.assertThat;
import com.github.lhotari.reactive.pulsar.producercache.CaffeineReactiveProducerCache;
import java.time.Duration;
import java.util.List;
import java.util.UUID;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.Schema;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

public class ReactiveMessageConsumerE2ETest {

    @Test
    void shouldConsumeMessages() throws PulsarClientException {
        try (PulsarClient pulsarClient = SingletonPulsarContainer.createPulsarClient();
             CaffeineReactiveProducerCache producerCache = new CaffeineReactiveProducerCache()) {
            String topicName = "test" + UUID.randomUUID();
            // create subscription to retain messages
            pulsarClient.newConsumer(Schema.STRING)
                    .topic(topicName)
                    .subscriptionName("sub")
                    .subscribe()
                    .close();

            ReactivePulsarClient reactivePulsarClient = ReactivePulsarClient.create(pulsarClient);

            ReactiveMessageSender<String> messageSender = reactivePulsarClient
                    .messageSender(Schema.STRING)
                    .cache(producerCache)
                    .topic(topicName)
                    .create();
            messageSender.sendMessages(Flux.range(1, 100)
                            .map(Object::toString)
                            .map(MessageSpec::of))
                    .blockLast();

            ReactiveMessageConsumer<String> messageConsumer =
                    reactivePulsarClient.messageConsumer(Schema.STRING)
                            .topic(topicName)
                            .consumerConfigurer(consumerBuilder -> consumerBuilder.subscriptionName("sub"))
                            .create();
            List<String> messages = messageConsumer.consumeMessages()
                    .map(consumedMessage -> {
                        consumedMessage.acknowledge();
                        return consumedMessage.getMessage().getValue();
                    })
                    .timeout(Duration.ofSeconds(2), Mono.empty())
                    .collectList().block();

            assertThat(messages)
                    .isEqualTo(Flux.range(1, 100).map(Object::toString).collectList().block());

            // should have acknowledged all messages
            List<ConsumedMessage<String>> remainingMessages = messageConsumer.consumeMessages()
                    .timeout(Duration.ofSeconds(2), Mono.empty())
                    .collectList()
                    .block();
            assertThat(remainingMessages).isEmpty();
        }
    }
}
