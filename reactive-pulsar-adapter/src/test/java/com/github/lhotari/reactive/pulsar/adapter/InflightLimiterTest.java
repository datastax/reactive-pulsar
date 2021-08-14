package com.github.lhotari.reactive.pulsar.adapter;

import static org.assertj.core.api.Assertions.assertThat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Flux;
import reactor.core.scheduler.Schedulers;
import reactor.test.StepVerifier;

class InflightLimiterTest {

    @Test
    void shouldLimitInflight() {
        List<Integer> values = Collections.synchronizedList(new ArrayList<>());
        InflightLimiter inflightLimiter = new InflightLimiter(48, 24, Schedulers.single());
        Flux.merge(Arrays.asList(Flux.range(1, 100)
                                .publishOn(Schedulers.parallel())
                                .log()
                                .as(inflightLimiter::createOperator),
                        Flux.range(101, 100)
                                .publishOn(Schedulers.parallel())
                                .log()
                                .as(inflightLimiter::createOperator),
                        Flux.range(201, 100)
                                .publishOn(Schedulers.parallel())
                                .log()
                                .as(inflightLimiter::createOperator)
                ))
                .as(StepVerifier::create)
                .expectSubscription()
                .recordWith(() -> values)
                .expectNextCount(300)
                .expectComplete()
                .verify();
        assertThat(values).containsExactlyInAnyOrderElementsOf(
                IntStream.range(1, 301).boxed().collect(Collectors.toList()));

        // verify "fairness"
        int previousValue = 1;
        for (int i = 0; i < values.size(); i++) {
            int value = values.get(i) % 100;
            if (value == 0) {
                value = 100;
            }
            assertThat(Math.abs(previousValue - value))
                    .as("value %d at index %d", values.get(i), i)
                    .isLessThanOrEqualTo(32);
            previousValue = value;
        }
    }

}