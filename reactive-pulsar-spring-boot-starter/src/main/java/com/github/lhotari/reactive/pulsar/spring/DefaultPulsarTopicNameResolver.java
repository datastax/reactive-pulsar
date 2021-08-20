package com.github.lhotari.reactive.pulsar.spring;

class DefaultPulsarTopicNameResolver implements PulsarTopicNameResolver {
    private final String pulsarTopicPrefix;

    public DefaultPulsarTopicNameResolver(String pulsarTopicPrefix) {
        this.pulsarTopicPrefix = pulsarTopicPrefix;
    }

    @Override
    public String resolveTopicName(String topicName) {
        if (topicName.contains("://")) {
            return topicName;
        } else {
            return pulsarTopicPrefix + topicName;
        }
    }
}
