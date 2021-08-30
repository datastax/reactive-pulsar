package com.github.lhotari.reactive.pulsar.adapter;

import static org.assertj.core.api.Assertions.assertThat;

import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.*;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.Schema;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.util.function.Tuple2;
import reactor.util.function.Tuples;

public class ReactiveMessageHandlerE2ETest {

    public static final int KEYS_COUNT = 100;
    public static final int ITEMS_PER_KEY_COUNT = 100;

    @Test
    void shouldConsumeMessages() throws Exception {
        try (PulsarClient pulsarClient = SingletonPulsarContainer.createPulsarClient()) {
            String topicName = "test" + UUID.randomUUID();
            // create subscription to retain messages
            pulsarClient.newConsumer(Schema.STRING).topic(topicName).subscriptionName("sub").subscribe().close();

            ReactivePulsarClient reactivePulsarClient = ReactivePulsarClient.create(pulsarClient);

            ReactiveMessageSender<String> messageSender = reactivePulsarClient
                .messageSender(Schema.STRING)
                .topic(topicName)
                .build();
            messageSender.sendMessages(Flux.range(1, 100).map(Object::toString).map(MessageSpec::of)).blockLast();

            List<String> messages = Collections.synchronizedList(new ArrayList<>());
            CountDownLatch latch = new CountDownLatch(100);

            try (
                ReactiveMessageHandler reactiveMessageHandler = ReactiveMessageHandlerBuilder
                    .builder(
                        reactivePulsarClient
                            .messageConsumer(Schema.STRING)
                            .consumerConfigurer(consumerBuilder ->
                                consumerBuilder.subscriptionName("sub").topic(topicName)
                            )
                            .build()
                    )
                    .messageHandler(message ->
                        Mono.fromRunnable(() -> {
                            messages.add(message.getValue());
                            latch.countDown();
                        })
                    )
                    .build()
                    .start()
            ) {
                latch.await(5, TimeUnit.SECONDS);
                assertThat(messages).isEqualTo(Flux.range(1, 100).map(Object::toString).collectList().block());
            }
        }
    }

    enum MessageOrderScenario {
        NO_PARALLEL,
        PARALLEL_PASS_KEY_IN_ORDERINGKEY,
        PARALLEL_PASS_KEY_IN_MESSAGEKEY
    }

    @ParameterizedTest
    @EnumSource(MessageOrderScenario.class)
    void shouldRetainMessageOrder(MessageOrderScenario messageOrderScenario) throws Exception {
        try (PulsarClient pulsarClient = SingletonPulsarContainer.createPulsarClient()) {
            String topicName = "test" + UUID.randomUUID();
            // create subscription to retain messages
            pulsarClient.newConsumer(Schema.INT32).topic(topicName).subscriptionName("sub").subscribe().close();

            ReactivePulsarClient reactivePulsarClient = ReactivePulsarClient.create(pulsarClient);

            ReactiveMessageSender<Integer> messageSender = reactivePulsarClient
                .messageSender(Schema.INT32)
                .topic(topicName)
                .build();

            List<MessageSpec<Integer>> messageSpecs = generateRandomOrderedMessagesWhereSingleKeyIsOrdered(
                messageOrderScenario
            );

            messageSender.sendMessages(Flux.fromIterable(messageSpecs)).blockLast();

            ConcurrentMap<Integer, List<Integer>> messages = new ConcurrentHashMap<>();
            CountDownLatch latch = new CountDownLatch(messageSpecs.size());

            List<Integer> orderedSequence = IntStream
                .rangeClosed(1, ITEMS_PER_KEY_COUNT)
                .boxed()
                .collect(Collectors.toList());

            ReactiveMessageHandlerBuilder.OneByOneMessageHandlerBuilder<Integer> reactiveMessageHandlerBuilder = ReactiveMessageHandlerBuilder
                .builder(
                    reactivePulsarClient
                        .messageConsumer(Schema.INT32)
                        .consumerConfigurer(consumerBuilder -> consumerBuilder.subscriptionName("sub").topic(topicName))
                        .build()
                )
                .messageHandler(message -> {
                    Mono<Void> messageHandler = Mono.fromRunnable(() -> {
                        Integer keyId = Integer.parseInt(message.getProperty("keyId"));
                        messages.compute(
                            keyId,
                            (k, list) -> {
                                if (list == null) {
                                    list = new ArrayList<>();
                                }
                                list.add(message.getValue());
                                return list;
                            }
                        );
                        latch.countDown();
                    });
                    if (messageOrderScenario != MessageOrderScenario.NO_PARALLEL) {
                        // add delay which would lead to the execution timeout unless
                        // messages are handled in parallel
                        messageHandler = Mono.delay(Duration.ofMillis(5)).then(messageHandler);
                    }
                    return messageHandler;
                });
            if (messageOrderScenario != MessageOrderScenario.NO_PARALLEL) {
                reactiveMessageHandlerBuilder.concurrent().concurrency(KEYS_COUNT).keyOrdered(true);
            }
            try (ReactiveMessageHandler reactiveMessageHandler = reactiveMessageHandlerBuilder.build().start()) {
                boolean latchCompleted = latch.await(5, TimeUnit.SECONDS);
                assertThat(latchCompleted).as("processing of all messages should have completed").isTrue();
                for (int i = 1; i <= KEYS_COUNT; i++) {
                    assertThat(messages.get(i)).as("keyId %d", i).containsExactlyElementsOf(orderedSequence);
                }
            }
        }
    }

    private List<MessageSpec<Integer>> generateRandomOrderedMessagesWhereSingleKeyIsOrdered(
        final MessageOrderScenario messageOrderScenario
    ) {
        List<Queue<MessageSpec<Integer>>> remainingMessages = Flux
            .range(1, KEYS_COUNT)
            .concatMap(keyId -> {
                String keyIdString = keyId.toString();
                byte[] keyBytes = ByteBuffer.allocate(4).putInt(keyId).array();
                return Flux
                    .range(1, ITEMS_PER_KEY_COUNT)
                    .map(i -> {
                        MessageSpecBuilder<Integer> messageSpecBuilder = MessageSpec
                            .builder(i)
                            .property("keyId", keyIdString);
                        switch (messageOrderScenario) {
                            case PARALLEL_PASS_KEY_IN_MESSAGEKEY:
                                messageSpecBuilder.key(keyIdString);
                                break;
                            case PARALLEL_PASS_KEY_IN_ORDERINGKEY:
                                messageSpecBuilder.orderingKey(keyBytes);
                                break;
                            case NO_PARALLEL:
                                break;
                        }
                        return Tuples.of(keyId, messageSpecBuilder.build());
                    });
            })
            .collectMultimap(Tuple2::getT1, Tuple2::getT2)
            .map(Map::values)
            .block()
            .stream()
            .map(LinkedBlockingQueue::new)
            .collect(Collectors.toList());

        List<MessageSpec<Integer>> messageSpecs = new ArrayList<>(KEYS_COUNT * ITEMS_PER_KEY_COUNT);
        while (messageSpecs.size() < KEYS_COUNT * ITEMS_PER_KEY_COUNT) {
            int randomIndex = ThreadLocalRandom.current().nextInt(remainingMessages.size());
            Queue<MessageSpec<Integer>> specsForKey = remainingMessages.get(randomIndex);
            MessageSpec<Integer> messageSpec = specsForKey.poll();
            messageSpecs.add(messageSpec);
            if (specsForKey.size() == 0) {
                remainingMessages.remove(randomIndex);
            }
        }
        return messageSpecs;
    }
}
