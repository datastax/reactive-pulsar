package com.github.lhotari.reactive.pulsar.spring.test;

import java.util.HashMap;
import java.util.Map;
import org.springframework.context.ApplicationContextInitializer;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.core.env.MapPropertySource;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.testcontainers.containers.PulsarContainer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.containers.wait.strategy.WaitAllStrategy;
import org.testcontainers.utility.DockerImageName;

/**
 * Wraps Testcontainer's {@link PulsarContainer} singleton instance.
 * This is to be used with Spring Tests by adding a test method
 * <pre>
 *     &#64;DynamicPropertySource
 *     static void registerPulsarProperties(DynamicPropertyRegistry registry) {
 *         SingletonPulsarContainer.register(registry);
 *     }
 * </pre>
 * or by adding {@code @ContextConfiguration(initializers = SingletonPulsarContainer.ContextInitializer.class)} to
 * the test class.
 */
public class SingletonPulsarContainer {

    public static void register(DynamicPropertyRegistry registry) {
        INSTANCE.registerToSpringTest(registry);
    }

    public static class ContextInitializer implements ApplicationContextInitializer<ConfigurableApplicationContext> {

        @Override
        public void initialize(ConfigurableApplicationContext applicationContext) {
            Map<String, Object> applicationProperties = new HashMap<>();
            register((name, valueSupplier) -> applicationProperties.put(name, valueSupplier.get()));
            MapPropertySource propertySource = new MapPropertySource(getClass().getName(), applicationProperties);
            applicationContext.getEnvironment().getPropertySources().addFirst(propertySource);
        }
    }

    private static final String PULSAR_CONTAINER_IMAGE = System
        .getenv()
        .getOrDefault("PULSAR_CONTAINER_IMAGE", "apachepulsar/pulsar:2.8.1");
    public static SingletonPulsarContainer INSTANCE = new SingletonPulsarContainer();

    private final PulsarContainer pulsarContainer = new PulsarContainer(
        DockerImageName.parse(PULSAR_CONTAINER_IMAGE).asCompatibleSubstituteFor("apachepulsar/pulsar")
    ) {
        @Override
        protected void configure() {
            super.configure();
            new WaitAllStrategy()
                .withStrategy(waitStrategy)
                .withStrategy(
                    Wait.forHttp("/admin/v2/namespaces/public/default").forPort(PulsarContainer.BROKER_HTTP_PORT)
                );
            withLabel("reactive-pulsar.container", "true");
            // enable container reuse feature
            withReuse(true);
            // default PULSAR_GC is very aggressive, https://github.com/apache/pulsar/blob/12f65663d3fa3d0a9005231ded3b25cd20d0d9f9/conf/pulsar_env.sh#L48
            // see discussion in https://github.com/apache/pulsar/pull/3650
            // pass simple PULSAR_GC setting instead
            withEnv("PULSAR_GC", "-XX:+UseG1GC");
        }
    };

    private SingletonPulsarContainer() {
        pulsarContainer.start();
    }

    private void registerToSpringTest(DynamicPropertyRegistry registry) {
        registerPulsarProperties(registry);
        registerTestTopicPrefix(registry);
    }

    private void registerPulsarProperties(DynamicPropertyRegistry registry) {
        registry.add("pulsar.client.serviceUrl", pulsarContainer::getPulsarBrokerUrl);
        // TODO: this property is currently unused
        registry.add("pulsar.admin.serviceHttpUrl", pulsarContainer::getHttpServiceUrl);
    }

    private void registerTestTopicPrefix(DynamicPropertyRegistry registry) {
        registry.add("pulsar.topicNamePrefix", this::createTestTopicPrefix);
    }

    private String createTestTopicPrefix() {
        return "test" + (System.currentTimeMillis() / 1000L) + "_";
    }

    public PulsarContainer getPulsarContainer() {
        return pulsarContainer;
    }
}
