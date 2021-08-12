package com.github.lhotari.reactive.pulsar.adapter;

import static org.assertj.core.api.Assertions.assertThat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.Schema;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

public class ReactiveConsumerE2ETest {

    @Test
    void shouldConsumeMessages() throws Exception {
        try (PulsarClient pulsarClient = SingletonPulsarContainer.createPulsarClient()) {
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
                    .topic(topicName)
                    .create();
            messageSender.sendMessages(Flux.range(1, 100)
                            .map(Object::toString)
                            .map(string -> MessageSpec.<String>builder().value(string).build()))
                    .blockLast();

            List<String> messages = Collections.synchronizedList(new ArrayList<>());
            CountDownLatch latch = new CountDownLatch(100);
            try (ReactiveConsumerPipeline reactiveConsumerPipeline =
                         reactivePulsarClient.pipeline(Schema.STRING)
                                 .consumerConfigurer(consumerBuilder ->
                                         consumerBuilder.subscriptionName("sub")
                                                 .topic(topicName))
                                 .messageHandler(message -> Mono.fromRunnable(() -> {
                                     messages.add(message.getValue());
                                     latch.countDown();
                                 }))
                                 .build()) {
                latch.await(5, TimeUnit.SECONDS);
                assertThat(messages)
                        .isEqualTo(Flux.range(1, 100).map(Object::toString).collectList().block());
            }
        }
    }
}
