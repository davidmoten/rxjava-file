package com.github.davidmoten.rx.operators;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.concurrent.atomic.AtomicLong;

import rx.Observable;
import rx.Observable.Operator;
import rx.Observer;
import rx.Subscriber;
import rx.functions.Func1;
import rx.observables.StringObservable;
import rx.observers.Subscribers;
import rx.subjects.PublishSubject;

/**
 * Reacts to source events by emitting new lines written to a file since the
 * last source event.
 */
public class OperatorFileTailer implements Operator<byte[], Object> {

    private final File file;
    private final AtomicLong currentPosition = new AtomicLong();

    /**
     * Constructor.
     * 
     * @param file
     *            text file to tail
     * @param startPosition
     *            start tailing the file after this many bytes
     */
    public OperatorFileTailer(File file, long startPosition) {
        this.file = file;
        this.currentPosition.set(startPosition);
    }

    @Override
    public Subscriber<? super Object> call(Subscriber<? super byte[]> subscriber) {
        final PublishSubject<? super Object> subject = PublishSubject.create();
        Subscriber<? super Object> result = Subscribers.from(subject);
        subscriber.add(result);
        subject
        // report new lines for each event
        .concatMap(reportNewLines(file, currentPosition))
        // subscribe
                .unsafeSubscribe(subscriber);
        return result;
    }

    private static Func1<Object, Observable<byte[]>> reportNewLines(final File file, final AtomicLong currentPosition) {
        return new Func1<Object, Observable<byte[]>>() {
            @Override
            public Observable<byte[]> call(Object event) {
                // TODO use Observable.create so can register close of input
                // stream on unsubscribe
                long length = file.length();
                if (length > currentPosition.get()) {
                    try {
                        final FileInputStream fis = new FileInputStream(file);
                        fis.skip(currentPosition.get());
                        // TODO allow option to vary buffer size?
                        return StringObservable.from(fis)
                        // handle moving file position and closing input stream
                        // TOOD close input stream on unsubscribe?
                                .doOnEach(moveMarkerAndCloseResources(currentPosition, fis));
                    } catch (IOException e) {
                        return Observable.error(e);
                    }
                } else {
                    // file has shrunk in size, reset the current
                    // position to
                    // detect when it grows next
                    currentPosition.set(length);
                    return Observable.empty();
                }
            }

            private Observer<byte[]> moveMarkerAndCloseResources(final AtomicLong currentPosition,
                    final FileInputStream fis) {
                return new Observer<byte[]>() {

                    @Override
                    public void onCompleted() {
                        close(fis);
                    }

                    @Override
                    public void onError(Throwable e) {
                        close(fis);
                    }

                    private void close(FileInputStream fis) {
                        try {
                            fis.close();
                        } catch (IOException e) {
                            // do nothing
                        }
                    }

                    @Override
                    public void onNext(byte[] bytes) {
                        currentPosition.addAndGet(bytes.length);
                    }
                };
            }
        };
    }

}
