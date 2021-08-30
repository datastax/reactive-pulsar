package com.github.lhotari.reactive.pulsar.resourcewrapper;

import org.reactivestreams.Publisher;
import reactor.core.Disposable;

public interface PublisherTransformer extends Disposable {
    static PublisherTransformer identity() {
        return new PublisherTransformer() {
            @Override
            public void dispose() {}

            @Override
            public <T> Publisher<T> transform(Publisher<T> publisher) {
                return publisher;
            }
        };
    }

    <T> Publisher<T> transform(Publisher<T> publisher);
}
