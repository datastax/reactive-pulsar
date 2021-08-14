package com.github.lhotari.reactive.pulsar.adapter;

import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import org.jctools.queues.MpmcArrayQueue;
import org.reactivestreams.Publisher;
import org.reactivestreams.Subscription;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.CoreSubscriber;
import reactor.core.publisher.BaseSubscriber;
import reactor.core.publisher.Flux;
import reactor.core.publisher.FluxOperator;
import reactor.core.publisher.Mono;
import reactor.core.publisher.MonoOperator;
import reactor.core.scheduler.Scheduler;
import reactor.core.scheduler.Schedulers;
import reactor.util.context.Context;

public class InflightLimiter implements PublisherTransformer {
    private static final Logger LOG = LoggerFactory.getLogger(InflightLimiter.class);
    MpmcArrayQueue<InflightLimiterSubscriber<?>> pendingSubscriptions = new MpmcArrayQueue<>(1024);
    private final AtomicInteger inflight = new AtomicInteger();
    private final AtomicInteger activeSubscriptions = new AtomicInteger();
    private final int maxInflight;
    private final int maxSubscriptionInflight;
    private final Scheduler.Worker triggerNextWorker;

    public InflightLimiter(int maxInflight) {
        this(maxInflight, maxInflight, Schedulers.single());
    }

    public InflightLimiter(int maxInflight, int maxSubscriptionInflight,
                           Scheduler triggerNextScheduler) {
        this.maxInflight = maxInflight;
        this.maxSubscriptionInflight = maxSubscriptionInflight;
        this.triggerNextWorker = triggerNextScheduler.createWorker();
        if (maxSubscriptionInflight > maxInflight) {
            throw new IllegalArgumentException("maxSubscriptionInflight must be equal or less than maxInflight.");
        }
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
        InflightLimiterSubscriber<I> subscriber = new InflightLimiterSubscriber<I>(actual);
        source.subscribe(subscriber);
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

    @Override
    public void dispose() {
        triggerNextWorker.dispose();
        pendingSubscriptions.drain(InflightLimiterSubscriber::cancel);
    }

    @Override
    public boolean isDisposed() {
        return triggerNextWorker.isDisposed();
    }

    private class InflightLimiterSubscriber<I> extends BaseSubscriber<I> {
        private final CoreSubscriber<? super I> actual;
        private AtomicLong requestedDemand = new AtomicLong();
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

        public InflightLimiterSubscriber(CoreSubscriber<? super I> actual) {
            this.actual = actual;
        }

        @Override
        public Context currentContext() {
            return actual.currentContext();
        }

        @Override
        protected void hookOnSubscribe(Subscription subscription) {

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
            if (requestedDemand.get() > 0 &&
                    inflightForSubscription.get() <= maxSubscriptionInflight / 2 &&
                    inflight.get() < maxInflight) {
                long maxRequest = Math.max(
                        Math.min(Math.min(Math.min(requestedDemand.get(),
                                                maxInflight - inflight.get()),
                                        maxSubscriptionInflight - inflightForSubscription.get()),
                                maxInflight / activeSubscriptions.get()),
                        1);
                inflight.addAndGet((int) maxRequest);
                requestedDemand.addAndGet(-maxRequest);
                inflightForSubscription.addAndGet((int) maxRequest);
                request(maxRequest);
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
