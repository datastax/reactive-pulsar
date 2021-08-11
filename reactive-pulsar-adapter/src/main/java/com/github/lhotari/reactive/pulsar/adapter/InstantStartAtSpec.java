package com.github.lhotari.reactive.pulsar.adapter;

import java.time.Instant;
import java.util.Objects;

public final class InstantStartAtSpec extends StartAtSpec {
    private final Instant instant;

    public InstantStartAtSpec(final Instant instant) {
        this.instant = instant;
    }

    public Instant getInstant() {
        return this.instant;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        InstantStartAtSpec that = (InstantStartAtSpec) o;
        return Objects.equals(instant, that.instant);
    }

    @Override
    public int hashCode() {
        return Objects.hash(instant);
    }

    @Override
    public String toString() {
        return "InstantStartAtSpec{" +
                "instant=" + instant +
                '}';
    }
}
