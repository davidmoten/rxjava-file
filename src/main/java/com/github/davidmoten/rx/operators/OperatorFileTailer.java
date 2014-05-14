package com.github.davidmoten.rx.operators;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.concurrent.atomic.AtomicLong;

import rx.Observable;
import rx.Observable.Operator;
import rx.Subscriber;
import rx.Subscription;
import rx.functions.Action0;
import rx.functions.Action1;
import rx.functions.Func0;
import rx.functions.Func1;
import rx.observables.StringObservable;
import rx.observers.Subscribers;
import rx.subjects.PublishSubject;
import rx.subscriptions.Subscriptions;

/**
 * Reacts to source events by emitting new lines written to a file since the
 * last source event.
 */
public class OperatorFileTailer implements Operator<byte[], Object> {

    private final File file;
    private final AtomicLong currentPosition = new AtomicLong();
    private final int maxBytesPerEmission;

    /**
     * Constructor.
     * 
     * @param file
     *            text file to tail
     * @param startPosition
     *            start tailing the file after this many bytes
     */
    public OperatorFileTailer(File file, long startPosition, int maxBytesPerEmission) {
        this.file = file;
        this.currentPosition.set(startPosition);
        this.maxBytesPerEmission = maxBytesPerEmission;
    }

    /**
     * Constructor. Emits byte arrays of up to 8*1024 bytes.
     * 
     * @param file
     * @param startPosition
     */
    public OperatorFileTailer(File file, long startPosition) {
        this(file, startPosition, 8 * 1024);
    }

    @Override
    public Subscriber<? super Object> call(Subscriber<? super byte[]> subscriber) {
        final PublishSubject<? super Object> subject = PublishSubject.create();
        Subscriber<? super Object> result = Subscribers.from(subject);
        subscriber.add(result);
        subject
        // report new lines for each event
        .concatMap(reportNewLines(file, currentPosition, maxBytesPerEmission))
        // subscribe
                .unsafeSubscribe(subscriber);
        return result;
    }

    private static Func1<Object, Observable<byte[]>> reportNewLines(final File file, final AtomicLong currentPosition,
            final int maxBytesPerEmission) {
        return new Func1<Object, Observable<byte[]>>() {
            @Override
            public Observable<byte[]> call(Object event) {

                long length = file.length();
                if (length > currentPosition.get()) {
                    try {
                        final FileInputStream fis = new FileInputStream(file);
                        fis.skip(currentPosition.get());
                        // TODO allow option to vary buffer size?

                        // apply using method to ensure fis is closed on
                        // termination or unsubscription
                        Func0<Subscription> subscriptionFactory = createSubscriptionFactory(fis);
                        Func1<Subscription, Observable<byte[]>> observableFactory = createObservableFactory(fis,
                                currentPosition, maxBytesPerEmission);
                        return Observable.using(subscriptionFactory, observableFactory);
                    } catch (IOException e) {
                        return Observable.error(e);
                    }
                } else {
                    // file has shrunk in size so has probably been
                    // rolled over, reset the current
                    // position to zero
                    currentPosition.set(0);
                    return Observable.empty();
                }
            }

        };
    }

    private static Func0<Subscription> createSubscriptionFactory(final InputStream is) {
        return new Func0<Subscription>() {

            @Override
            public Subscription call() {
                return Subscriptions.create(new Action0() {
                    @Override
                    public void call() {
                        try {
                            is.close();
                        } catch (IOException e) {
                            throw new RuntimeException(e);
                        }
                    }
                });
            }
        };
    }

    private static Func1<Subscription, Observable<byte[]>> createObservableFactory(final FileInputStream fis,
            final AtomicLong currentPosition, final int maxBytesPerEmission) {
        return new Func1<Subscription, Observable<byte[]>>() {

            @Override
            public Observable<byte[]> call(Subscription subscription) {
                return StringObservable.from(fis, maxBytesPerEmission)
                // move marker
                        .doOnNext(new Action1<byte[]>() {
                            @Override
                            public void call(byte[] bytes) {
                                currentPosition.addAndGet(bytes.length);
                            }
                        });
            }
        };
    }

}
