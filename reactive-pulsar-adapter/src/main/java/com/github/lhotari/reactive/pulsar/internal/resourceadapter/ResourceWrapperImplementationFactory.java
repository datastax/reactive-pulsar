package com.github.lhotari.reactive.pulsar.internal.resourceadapter;

import com.github.lhotari.reactive.pulsar.resourceadapter.ReactivePulsarResourceAdapter;
import java.util.function.Supplier;
import org.apache.pulsar.client.api.PulsarClient;

public class ResourceWrapperImplementationFactory {

    public static ReactivePulsarResourceAdapter createReactivePulsarResourceAdapter(
        Supplier<PulsarClient> pulsarClientSupplier
    ) {
        return new DefaultReactivePulsarResourceAdapter(pulsarClientSupplier);
    }
}
