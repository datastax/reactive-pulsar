package com.github.lhotari.reactive.pulsar.spring;

class DefaultPulsarTopicNameResolver implements PulsarTopicNameResolver {
    private final String pulsarTopicNamePrefix;

    public DefaultPulsarTopicNameResolver(String pulsarTopicNamePrefix) {
        this.pulsarTopicNamePrefix = pulsarTopicNamePrefix;
    }

    @Override
    public String resolveTopicName(String topicName) {
        if (topicName.contains("://")) {
            return topicName;
        } else {
            return pulsarTopicNamePrefix + topicName;
        }
    }
}
