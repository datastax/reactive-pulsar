package com.github.lhotari.reactive.pulsar.spring;

import com.github.lhotari.reactive.pulsar.adapter.ReactivePulsarClient;
import com.github.lhotari.reactive.pulsar.producercache.CaffeineReactiveProducerCache;
import com.github.lhotari.reactive.pulsar.resourceadapter.ReactiveProducerCache;
import com.github.lhotari.reactive.pulsar.resourceadapter.ReactivePulsarResourceAdapter;
import org.apache.pulsar.client.api.PulsarClient;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration(proxyBeanMethods = false)
public class PulsarReactiveAdapterAutoConfiguration {

    @Bean
    @ConditionalOnMissingBean
    ReactivePulsarResourceAdapter ReactivePulsarResourceAdapter(
        ObjectProvider<PulsarClient> pulsarClientObjectProvider
    ) {
        return ReactivePulsarResourceAdapter.create(pulsarClientObjectProvider::getObject);
    }

    @Bean
    @ConditionalOnMissingBean
    ReactivePulsarClient reactivePulsarClient(ReactivePulsarResourceAdapter reactivePulsarResourceAdapter) {
        return ReactivePulsarClient.create(reactivePulsarResourceAdapter);
    }

    @Bean
    @ConditionalOnMissingBean
    ReactiveProducerCache reactiveProducerCache() {
        return new CaffeineReactiveProducerCache();
    }
}
