package com.github.lhotari.reactive.pulsar.spring;

import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.impl.ClientBuilderImpl;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Lazy;

@Configuration(proxyBeanMethods = false)
@EnableConfigurationProperties(PulsarClientConfig.class)
public class PulsarClientAutoConfiguration {

    @Lazy
    @Bean
    @ConditionalOnMissingBean
    PulsarClient pulsarClient(PulsarClientConfig pulsarClientConfig) throws PulsarClientException {
        return new ClientBuilderImpl(pulsarClientConfig).build();
    }

    @Bean
    @ConditionalOnMissingBean
    PulsarTopicNameResolver pulsarTopicNameResolver(
        @Value("${pulsar.topicNamePrefix:persistent://public/default/}") String pulsarTopicPrefix
    ) {
        return new DefaultPulsarTopicNameResolver(pulsarTopicPrefix);
    }
}
