package com.github.lhotari.reactive.pulsar.internal.resourcewrapper;

import com.github.lhotari.reactive.pulsar.resourcewrapper.ProducerCacheKey;
import java.util.Objects;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.impl.conf.ProducerConfigurationData;

final class DefaultProducerCacheKey implements ProducerCacheKey {

    private final PulsarClient pulsarClient;
    private final ProducerConfigurationData producerConfigurationData;
    private final Schema<?> schema;

    DefaultProducerCacheKey(
        final PulsarClient pulsarClient,
        final ProducerConfigurationData producerConfigurationData,
        final Schema<?> schema
    ) {
        this.pulsarClient = pulsarClient;
        this.producerConfigurationData = producerConfigurationData;
        this.schema = schema;
    }

    public String getTopicName() {
        return this.producerConfigurationData != null ? this.producerConfigurationData.getTopicName() : null;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        DefaultProducerCacheKey that = (DefaultProducerCacheKey) o;
        return (
            Objects.equals(pulsarClient, that.pulsarClient) &&
            Objects.equals(producerConfigurationData, that.producerConfigurationData) &&
            Objects.equals(schema, that.schema)
        );
    }

    @Override
    public int hashCode() {
        return Objects.hash(pulsarClient, producerConfigurationData, schema);
    }
}
