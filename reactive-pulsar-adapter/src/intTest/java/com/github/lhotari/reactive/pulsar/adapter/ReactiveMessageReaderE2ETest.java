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

            ReactivePulsarAdapter reactivePulsarAdapter = ReactivePulsarAdapter.create(pulsarClient);

            ReactiveMessageSender<String> messageSender = reactivePulsarAdapter
                    .producer()
                    .cache(producerCache)
                    .messageSender(Schema.STRING)
                    .topic(topicName)
                    .create();
            messageSender.sendMessagePayloads(Flux.range(1, 100).map(Object::toString))
                    .blockLast();

            ReactiveMessageReader<String> messageReader =
                    reactivePulsarAdapter.reader().messageReader(Schema.STRING)
                            .topic(topicName)
                            .create();
            List<String> messages = messageReader.readMessages(StartAtSpec.ofEarliest(), EndOfStreamAction.COMPLETE)
                    .map(Message::getValue).collectList().block();

            assertThat(messages)
                    .isEqualTo(Flux.range(1, 100).map(Object::toString).collectList().block());
        }
    }
}
