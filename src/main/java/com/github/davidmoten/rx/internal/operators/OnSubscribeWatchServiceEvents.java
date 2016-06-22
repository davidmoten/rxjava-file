package com.github.davidmoten.rx.internal.operators;

import java.io.IOException;
import java.nio.file.ClosedWatchServiceException;
import java.nio.file.WatchEvent;
import java.nio.file.WatchKey;
import java.nio.file.WatchService;
import java.util.concurrent.TimeUnit;

import rx.Observable.OnSubscribe;
import rx.Scheduler;
import rx.Scheduler.Worker;
import rx.Subscriber;
import rx.Subscription;
import rx.functions.Action0;
import rx.subscriptions.Subscriptions;

public final class OnSubscribeWatchServiceEvents implements OnSubscribe<WatchEvent<?>> {

    private final Scheduler scheduler;
    private final WatchService watchService;
    private final long pollDurationMs;
    private final long pollIntervalMs;

    public OnSubscribeWatchServiceEvents(WatchService watchService, Scheduler scheduler,
            long pollDuration, TimeUnit pollDurationUnit, long pollInterval,
            TimeUnit pollIntervalUnit) {
        this.watchService = watchService;
        this.scheduler = scheduler;
        this.pollDurationMs = pollDurationUnit.toMillis(pollDuration);
        this.pollIntervalMs = pollIntervalUnit.toMillis(pollInterval);
    }

    @Override
    public void call(final Subscriber<? super WatchEvent<?>> subscriber) {
        final Worker worker = scheduler.createWorker();
        subscriber.add(worker);
        subscriber.add(createSubscriptionToCloseWatchService(watchService));
        worker.schedule(new Action0() {
            @Override
            public void call() {
                if (emitEvents(watchService, subscriber, pollDurationMs, pollIntervalMs)) {
                    worker.schedule(this);
                }
            }
        }, pollIntervalMs, TimeUnit.MILLISECONDS);
    }

    // returns true if and only there may be more events
    private static boolean emitEvents(WatchService watchService,
            Subscriber<? super WatchEvent<?>> subscriber, long pollDurationMs,
            long pollIntervalMs) {
        // get the first event
        WatchKey key = nextKey(watchService, subscriber, pollDurationMs);

        if (key != null) {
            if (subscriber.isUnsubscribed())
                return false;
            // we have a polled event, now we traverse it and
            // receive all the states from it
            for (WatchEvent<?> event : key.pollEvents()) {
                if (subscriber.isUnsubscribed())
                    return false;
                else
                    subscriber.onNext(event);
            }

            boolean valid = key.reset();
            if (!valid && !subscriber.isUnsubscribed()) {
                subscriber.onCompleted();
                return false;
            } else if (!valid)
                return false;
        }
        return true;
    }

    private static WatchKey nextKey(WatchService watchService,
            Subscriber<? super WatchEvent<?>> subscriber, long pollDurationMs) {
        try {
            // this command blocks but unsubscribe closes the watch
            // service and interrupts it
            if (pollDurationMs == 0) {
                return watchService.poll();
            } else if (pollDurationMs == Long.MAX_VALUE) {
                // blocking
                return watchService.take();
            } else {
                // blocking
                return watchService.poll(pollDurationMs, TimeUnit.MILLISECONDS);
            }
        } catch (ClosedWatchServiceException e) {
            // must have unsubscribed
            if (!subscriber.isUnsubscribed())
                subscriber.onCompleted();
            return null;
        } catch (InterruptedException e) {
            // this case is problematic because unsubscribe may call
            // Thread.interrupt() before calling the unsubscribe method of
            // the Subscription. Thus at this point we don't know if a
            // deliberate interrupt was called in which case I would call
            // onComplete or if unsubscribe was called in which case I
            // should not call anything. For the moment I choose to not call
            // anything partly because a deliberate stop of the
            // watchService.take ignorant of the Observable should ideally
            // happen via a call to the WatchService.close() method rather
            // than Thread.interrupt().
            // TODO raise the issue with RxJava team in particular
            // Subscriptions.from(Future) calling FutureTask.cancel(true)
            try {
                watchService.close();
            } catch (IOException e1) {
                // do nothing
            }
            return null;
        }
    }

    private final static Subscription createSubscriptionToCloseWatchService(
            final WatchService watchService) {
        return Subscriptions.create(new Action0() {

            @Override
            public void call() {
                try {
                    watchService.close();
                } catch (ClosedWatchServiceException e) {
                    // do nothing
                } catch (IOException e) {
                    // do nothing
                }
            }
        });
    }
}
