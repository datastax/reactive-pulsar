package com.github.lhotari.reactive.pulsar.internal.adapter;

import com.github.lhotari.reactive.pulsar.resourceadapter.PublisherTransformer;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import org.jctools.queues.MpmcArrayQueue;
import org.reactivestreams.Publisher;
import org.reactivestreams.Subscription;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.CoreSubscriber;
import reactor.core.publisher.*;
import reactor.core.scheduler.Scheduler;
import reactor.core.scheduler.Schedulers;
import reactor.util.context.Context;

class InflightLimiter implements PublisherTransformer {

    private static final Logger LOG = LoggerFactory.getLogger(InflightLimiter.class);
    public static final int DEFAULT_MAX_PENDING_SUBSCRIPTIONS = 1024;
    private final MpmcArrayQueue<InflightLimiterSubscriber<?>> pendingSubscriptions;
    private final AtomicInteger inflight = new AtomicInteger();
    private final AtomicInteger activeSubscriptions = new AtomicInteger();
    private final int maxInflight;
    private final int expectedSubscriptionsInflight;
    private final Scheduler.Worker triggerNextWorker;

    public InflightLimiter(int maxInflight) {
        this(maxInflight, maxInflight, Schedulers.single(), DEFAULT_MAX_PENDING_SUBSCRIPTIONS);
    }

    public InflightLimiter(
        int maxInflight,
        int expectedSubscriptionsInflight,
        Scheduler triggerNextScheduler,
        int maxPendingSubscriptions
    ) {
        this.maxInflight = maxInflight;
        this.expectedSubscriptionsInflight = expectedSubscriptionsInflight;
        this.triggerNextWorker = triggerNextScheduler.createWorker();
        if (expectedSubscriptionsInflight > maxInflight) {
            throw new IllegalArgumentException("maxSubscriptionInflight must be equal or less than maxInflight.");
        }
        this.pendingSubscriptions = new MpmcArrayQueue<>(maxPendingSubscriptions);
    }

    @Override
    public <T> Publisher<T> transform(Publisher<T> publisher) {
        if (publisher instanceof Mono<?>) {
            return createOperator(Mono.class.cast(publisher));
        } else {
            return createOperator(Flux.from(publisher));
        }
    }

    public <I> Flux<I> createOperator(Flux<I> source) {
        return new FluxOperator<I, I>(source) {
            @Override
            public void subscribe(CoreSubscriber<? super I> actual) {
                handleSubscribe(source, actual);
            }
        };
    }

    public <I> Mono<I> createOperator(Mono<I> source) {
        return new MonoOperator<I, I>(source) {
            @Override
            public void subscribe(CoreSubscriber<? super I> actual) {
                handleSubscribe(source, actual);
            }
        };
    }

    <I> void handleSubscribe(Publisher<I> source, CoreSubscriber<? super I> actual) {
        activeSubscriptions.incrementAndGet();
        InflightLimiterSubscriber<I> subscriber = new InflightLimiterSubscriber<I>(actual, source);
        actual.onSubscribe(subscriber.getSubscription());
    }

    void maybeTriggerNext() {
        if (!triggerNextWorker.isDisposed()) {
            triggerNextWorker.schedule(() -> {
                int remainingSubscriptions = pendingSubscriptions.size();
                while (inflight.get() < maxInflight && remainingSubscriptions-- > 0) {
                    InflightLimiterSubscriber<?> subscriber = pendingSubscriptions.poll();
                    if (subscriber != null) {
                        if (!subscriber.isDisposed()) {
                            subscriber.requestMore();
                        }
                    } else {
                        break;
                    }
                }
            });
        }
    }

    void scheduleSubscribed(InflightLimiterSubscriber<?> subscriber) {
        if (!triggerNextWorker.isDisposed()) {
            triggerNextWorker.schedule(() -> {
                if (!subscriber.isDisposed()) {
                    subscriber.requestMore();
                }
            });
        }
    }

    @Override
    public void dispose() {
        triggerNextWorker.dispose();
        pendingSubscriptions.drain(InflightLimiterSubscriber::cancel);
    }

    @Override
    public boolean isDisposed() {
        return triggerNextWorker.isDisposed();
    }

    private enum InflightLimiterSubscriberState {
        INITIAL,
        SUBSCRIBING,
        SUBSCRIBED,
        REQUESTING
    }

    private class InflightLimiterSubscriber<I> extends BaseSubscriber<I> {

        private final CoreSubscriber<? super I> actual;
        private final Publisher<I> source;
        private AtomicLong requestedDemand = new AtomicLong();
        private AtomicReference<InflightLimiterSubscriberState> state = new AtomicReference<>(
            InflightLimiterSubscriberState.INITIAL
        );

        private final Subscription subscription = new Subscription() {
            @Override
            public void request(long n) {
                requestedDemand.addAndGet(n);
                maybeAddToPending();
                maybeTriggerNext();
            }

            @Override
            public void cancel() {
                InflightLimiterSubscriber.this.cancel();
            }
        };
        private AtomicInteger inflightForSubscription = new AtomicInteger();

        public InflightLimiterSubscriber(CoreSubscriber<? super I> actual, Publisher<I> source) {
            this.actual = actual;
            this.source = source;
        }

        @Override
        public Context currentContext() {
            return actual.currentContext();
        }

        @Override
        protected void hookOnSubscribe(Subscription subscription) {
            if (
                state.compareAndSet(
                    InflightLimiterSubscriberState.SUBSCRIBING,
                    InflightLimiterSubscriberState.SUBSCRIBED
                )
            ) {
                scheduleSubscribed(this);
            }
        }

        @Override
        protected void hookOnNext(I value) {
            actual.onNext(value);
            inflight.decrementAndGet();
            inflightForSubscription.decrementAndGet();
            maybeAddToPending();
            maybeTriggerNext();
        }

        @Override
        protected void hookOnComplete() {
            activeSubscriptions.decrementAndGet();
            actual.onComplete();
            clearInflight();
            maybeTriggerNext();
        }

        private void clearInflight() {
            inflight.addAndGet(-inflightForSubscription.getAndSet(0));
        }

        @Override
        protected void hookOnError(Throwable throwable) {
            activeSubscriptions.decrementAndGet();
            actual.onError(throwable);
            clearInflight();
            maybeTriggerNext();
        }

        @Override
        protected void hookOnCancel() {
            activeSubscriptions.decrementAndGet();
            clearInflight();
            requestedDemand.set(0);
            maybeTriggerNext();
        }

        public Subscription getSubscription() {
            return subscription;
        }

        void requestMore() {
            if (
                state.get() == InflightLimiterSubscriberState.SUBSCRIBED ||
                (
                    requestedDemand.get() > 0 &&
                    inflightForSubscription.get() <= expectedSubscriptionsInflight / 2 &&
                    inflight.get() < maxInflight
                )
            ) {
                if (
                    state.compareAndSet(
                        InflightLimiterSubscriberState.INITIAL,
                        InflightLimiterSubscriberState.SUBSCRIBING
                    )
                ) {
                    // consume one slot for the subscription, since the first element might already be in flight
                    // when a CompletableFuture is mapped to a Mono
                    inflight.incrementAndGet();
                    requestedDemand.decrementAndGet();
                    inflightForSubscription.incrementAndGet();
                    source.subscribe(InflightLimiterSubscriber.this);
                } else if (
                    state.get() == InflightLimiterSubscriberState.REQUESTING ||
                    state.get() == InflightLimiterSubscriberState.SUBSCRIBED
                ) {
                    // subscribing changed the values, so adjust back the values on first call
                    if (
                        state.compareAndSet(
                            InflightLimiterSubscriberState.SUBSCRIBED,
                            InflightLimiterSubscriberState.REQUESTING
                        )
                    ) {
                        // reverse the slot reservation made when transitioning from INITIAL to SUBSCRIBING
                        inflight.decrementAndGet();
                        requestedDemand.incrementAndGet();
                        inflightForSubscription.decrementAndGet();
                    }
                    long maxRequest = Math.max(
                        Math.min(
                            Math.min(
                                Math.min(requestedDemand.get(), maxInflight - inflight.get()),
                                expectedSubscriptionsInflight - inflightForSubscription.get()
                            ),
                            maxInflight / Math.max(activeSubscriptions.get(), 1)
                        ),
                        1
                    );
                    inflight.addAndGet((int) maxRequest);
                    requestedDemand.addAndGet(-maxRequest);
                    inflightForSubscription.addAndGet((int) maxRequest);
                    request(maxRequest);
                }
            } else {
                maybeAddToPending();
            }
        }

        void maybeAddToPending() {
            if (requestedDemand.get() > 0 && !isDisposed() && inflightForSubscription.get() == 0) {
                pendingSubscriptions.add(this);
            }
        }
    }
}
