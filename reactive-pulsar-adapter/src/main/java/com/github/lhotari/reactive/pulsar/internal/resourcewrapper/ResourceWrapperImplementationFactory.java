package com.github.lhotari.reactive.pulsar.internal.resourcewrapper;

import com.github.lhotari.reactive.pulsar.resourcewrapper.ReactivePulsarAdapter;
import java.util.function.Supplier;
import org.apache.pulsar.client.api.PulsarClient;

public class ResourceWrapperImplementationFactory {

    public static ReactivePulsarAdapter createReactivePulsarAdapter(Supplier<PulsarClient> pulsarClientSupplier) {
        return new DefaultReactivePulsarAdapter(pulsarClientSupplier);
    }
}
