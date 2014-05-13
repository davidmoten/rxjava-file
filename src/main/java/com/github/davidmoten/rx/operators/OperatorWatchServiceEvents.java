package com.github.davidmoten.rx.operators;

import java.io.IOException;
import java.nio.file.ClosedWatchServiceException;
import java.nio.file.WatchEvent;
import java.nio.file.WatchKey;
import java.nio.file.WatchService;
import java.util.concurrent.atomic.AtomicBoolean;

import rx.Observable.Operator;
import rx.Observer;
import rx.Subscriber;
import rx.Subscription;
import rx.observers.Subscribers;

/**
 * Emits {@link WatchEvent}s for each input {@link WatchService}.
 */
public class OperatorWatchServiceEvents implements Operator<WatchEvent<?>, WatchService> {

    @Override
    public Subscriber<? super WatchService> call(final Subscriber<? super WatchEvent<?>> subscriber) {
        Subscriber<WatchService> result = Subscribers.from(new Observer<WatchService>() {

            @Override
            public void onCompleted() {
                subscriber.onCompleted();
            }

            @Override
            public void onError(Throwable e) {
                subscriber.onError(e);
            }

            @Override
            public void onNext(WatchService watchService) {
                AtomicBoolean subscribed = new AtomicBoolean(true);
                if (!subscribed.get()) {
                    subscriber.onError(new RuntimeException(
                            "WatchService closed. You can only subscribe once to a WatchService."));
                    return;
                }
                subscriber.add(createSubscriptionToCloseWatchService(watchService, subscribed,
                        subscriber));
                emitEvents(watchService, subscriber, subscribed);
            }
        });
        subscriber.add(result);
        return result;
    }

    private static void emitEvents(WatchService watchService,
            Subscriber<? super WatchEvent<?>> subscriber, AtomicBoolean subscribed) {
        // get the first event before looping
        WatchKey key = nextKey(watchService, subscriber, subscribed);

        while (key != null) {
            if (subscriber.isUnsubscribed())
                return;
            // we have a polled event, now we traverse it and
            // receive all the states from it
            for (WatchEvent<?> event : key.pollEvents()) {
                if (subscriber.isUnsubscribed())
                    return;
                else
                    subscriber.onNext(event);
            }

            boolean valid = key.reset();
            if (!valid && subscribed.get()) {
                subscriber.onCompleted();
                return;
            } else if (!valid)
                return;

            key = nextKey(watchService, subscriber, subscribed);
        }
    }

    private static WatchKey nextKey(WatchService watchService,
            Subscriber<? super WatchEvent<?>> subscriber, AtomicBoolean subscribed) {
        try {
            // this command blocks but unsubscribe close the watch
            // service and interrupt it
            return watchService.take();
        } catch (ClosedWatchServiceException e) {
            // must have unsubscribed
            if (subscribed.get())
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
            final WatchService watchService, final AtomicBoolean subscribed,
            final Subscriber<? super WatchEvent<?>> subscriber) {
        return new Subscription() {

            @Override
            public void unsubscribe() {
                try {
                    watchService.close();
                } catch (ClosedWatchServiceException e) {
                    // do nothing
                } catch (IOException e) {
                    // do nothing
                } finally {
                    subscribed.set(false);
                }
            }

            @Override
            public boolean isUnsubscribed() {
                return !subscribed.get();
            }
        };
    }

}
