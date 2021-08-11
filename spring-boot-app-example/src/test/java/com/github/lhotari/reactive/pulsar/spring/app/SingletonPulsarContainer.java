package com.github.lhotari.reactive.pulsar.spring.app;

import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.testcontainers.containers.PulsarContainer;
import org.testcontainers.utility.DockerImageName;

public class SingletonPulsarContainer {
    public static PulsarContainer PULSAR_CONTAINER = new PulsarContainer(DockerImageName.parse("apachepulsar/pulsar")
            .withTag("2.8.0"));

    static {
        PULSAR_CONTAINER.start();
    }

    public static PulsarClient createPulsarClient() throws PulsarClientException {
        return PulsarClient.builder()
                .serviceUrl(SingletonPulsarContainer.PULSAR_CONTAINER.getPulsarBrokerUrl())
                .build();
    }

    public static void registerPulsarProperties(DynamicPropertyRegistry registry) {
        registry.add("pulsar.client.serviceUrl",
                SingletonPulsarContainer.PULSAR_CONTAINER::getPulsarBrokerUrl);
    }
}
