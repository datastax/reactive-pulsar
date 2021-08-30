package com.github.lhotari.reactive.pulsar.adapter;

import static org.assertj.core.api.Assertions.assertThat;
import com.github.lhotari.reactive.pulsar.producercache.CaffeineReactiveProducerCache;
import java.util.List;
import java.util.UUID;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.Schema;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Flux;

public class ReactiveMessageReaderE2ETest {

    @Test
    void shouldReadMessages() throws PulsarClientException {
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
                    .build();
            messageSender.sendMessages(Flux.range(1, 100)
                            .map(Object::toString)
                            .map(MessageSpec::of))
                    .blockLast();

            ReactiveMessageReader<String> messageReader =
                    reactivePulsarClient.messageReader(Schema.STRING)
                            .topic(topicName)
                            .build();
            List<String> messages = messageReader.readMessages()
                    .map(Message::getValue).collectList().block();

            assertThat(messages)
                    .isEqualTo(Flux.range(1, 100).map(Object::toString).collectList().block());
        }
    }
}
