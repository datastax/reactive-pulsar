package com.github.lhotari.reactive.pulsar.spring;

import org.apache.pulsar.client.api.AuthenticationFactory;
import org.apache.pulsar.client.impl.conf.ClientConfigurationData;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.boot.context.properties.ConfigurationProperties;

@ConfigurationProperties("pulsar.client")
public class PulsarClientConfig extends ClientConfigurationData implements InitializingBean {
    public PulsarClientConfig() {
    }

    void setIoThreads(int ioThreads) {
        setNumIoThreads(ioThreads);
    }

    void setListenerThreads(int listenerThreads) {
        setNumListenerThreads(listenerThreads);
    }

    void setMaxLookupRequests(int maxLookupRequests) {
        setMaxLookupRequest(maxLookupRequests);
    }

    void setMaxConcurrentLookupRequests(int maxConcurrentLookupRequests) {
        setConcurrentLookupRequest(maxConcurrentLookupRequests);
    }

    @Override
    public void afterPropertiesSet() throws Exception {
        if (getAuthPluginClassName() != null) {
            if (getAuthParams() != null) {
                setAuthentication(AuthenticationFactory.create(getAuthPluginClassName(), getAuthParams()));
            } else if (getAuthParamMap() != null) {
                setAuthentication(AuthenticationFactory.create(getAuthPluginClassName(), getAuthParamMap()));
            }
        }
    }
}