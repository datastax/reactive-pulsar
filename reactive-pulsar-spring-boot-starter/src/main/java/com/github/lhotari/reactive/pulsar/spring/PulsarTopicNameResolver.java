package com.github.lhotari.reactive.pulsar.spring;

/**
 * Interface that eases the integration testing of applications that use Pulsar topics.
 * When run under a test, the topic name can be changed between test runs by the test infrastructure.
 */
public interface PulsarTopicNameResolver {
    String resolveTopicName(String topicName);
}
