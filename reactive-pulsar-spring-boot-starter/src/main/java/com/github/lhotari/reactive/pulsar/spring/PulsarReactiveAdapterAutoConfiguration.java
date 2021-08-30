package com.github.lhotari.reactive.pulsar.spring;

import com.github.lhotari.reactive.pulsar.adapter.ReactiveProducerCache;
import com.github.lhotari.reactive.pulsar.adapter.ReactivePulsarAdapter;
import com.github.lhotari.reactive.pulsar.adapter.ReactivePulsarClient;
import com.github.lhotari.reactive.pulsar.producercache.CaffeineReactiveProducerCache;
import org.apache.pulsar.client.api.PulsarClient;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration(proxyBeanMethods = false)
public class PulsarReactiveAdapterAutoConfiguration {

    @Bean
    @ConditionalOnMissingBean
    ReactivePulsarAdapter reactivePulsarAdapter(ObjectProvider<PulsarClient> pulsarClientObjectProvider) {
        return ReactivePulsarAdapter.create(pulsarClientObjectProvider::getObject);
    }

    @Bean
    @ConditionalOnMissingBean
    ReactivePulsarClient reactivePulsarClient(ReactivePulsarAdapter reactivePulsarAdapter) {
        return ReactivePulsarClient.create(reactivePulsarAdapter);
    }

    @Bean
    @ConditionalOnMissingBean
    ReactiveProducerCache reactiveProducerCache() {
        return new CaffeineReactiveProducerCache();
    }
}
