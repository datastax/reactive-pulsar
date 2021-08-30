package com.github.lhotari.reactive.pulsar.internal.resourceadapter;

import com.github.lhotari.reactive.pulsar.resourceadapter.ReactivePulsarAdapter;
import java.util.function.Supplier;
import org.apache.pulsar.client.api.PulsarClient;

public class ResourceWrapperImplementationFactory {

    public static ReactivePulsarAdapter createReactivePulsarAdapter(Supplier<PulsarClient> pulsarClientSupplier) {
        return new DefaultReactivePulsarAdapter(pulsarClientSupplier);
    }
}
