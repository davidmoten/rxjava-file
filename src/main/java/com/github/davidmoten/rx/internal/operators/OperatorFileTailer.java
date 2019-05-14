package com.github.davidmoten.rx.internal.operators;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardWatchEventKinds;
import java.nio.file.WatchEvent;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicLong;

import com.github.davidmoten.rx.Bytes;
import com.github.davidmoten.rx.subjects.PublishSubjectSingleSubscriber;

import rx.Observable;
import rx.Observable.Operator;
import rx.Subscriber;
import rx.functions.Action1;
import rx.functions.Func0;
import rx.functions.Func1;
import rx.observers.Subscribers;

/**
 * Reacts to source events by emitting new lines written to a file since the
 * last source event.
 */
public class OperatorFileTailer implements Operator<byte[], Object> {

    private final Path path;
    private final AtomicLong currentPosition = new AtomicLong();
    private final int maxBytesPerEmission;

    /**
     * Constructor. Emits byte arrays of up to 8*1024 bytes.
     * 
     * @param file
     *            file to tail
     * @param startPosition
     *            start position in bytes
     */
    public OperatorFileTailer(File file, long startPosition) {
        this(file, startPosition, 8192);
    }
    
    /**
     * Constructor.
     * 
     * @param file
     *            text file to tail
     * @param startPosition
     *            start tailing the file after this many bytes
     * @param maxBytesPerEmission
     *            maximum number of bytes per emission
     */
    public OperatorFileTailer(File file, long startPosition, int maxBytesPerEmission) {
    	this(Objects.requireNonNull(file, "file cannot be null").toPath(), startPosition, maxBytesPerEmission);
    }
    
    public OperatorFileTailer(Path path, long startPosition, int maxBytesPerEmission) {
        this.path = Objects.requireNonNull(path, "file cannot be null");
        this.currentPosition.set(startPosition);
        this.maxBytesPerEmission = maxBytesPerEmission;
    }

    @Override
    public Subscriber<? super Object> call(Subscriber<? super byte[]> child) {
        final PublishSubjectSingleSubscriber<? super Object> subject = PublishSubjectSingleSubscriber
                .create();
        Subscriber<? super Object> parent = Subscribers.from(subject);
        child.add(parent);
        subject
                // report new lines for each event
                .concatMap(reportNewLines(path, currentPosition, maxBytesPerEmission))
                // subscribe
                .unsafeSubscribe(child);
        return parent;
    }

    private static Func1<Object, Observable<byte[]>> reportNewLines(final Path path,
            final AtomicLong currentPosition, final int maxBytesPerEmission) {
        return new Func1<Object, Observable<byte[]>>() {
            @Override
            public Observable<byte[]> call(Object event) {
                // reset current position if file is moved or deleted
                if (event instanceof WatchEvent) {
                    WatchEvent<?> w = (WatchEvent<?>) event;
                    String kind = w.kind().name();
                    if (kind.equals(StandardWatchEventKinds.ENTRY_CREATE.name())) {
                        currentPosition.set(0);
                    }
                }
                long length;
				try {
					length = Files.size(path);
				} catch (IOException e) {
					return Observable.error(e);
				}
                if (length > currentPosition.get()) {
                    try {
                        final InputStream fis = Files.newInputStream(path);
                        fis.skip(currentPosition.get());
                        // apply using method to ensure fis is closed on
                        // termination or unsubscription
                        return Observable.using(new Func0<InputStream>() {

                            @Override
                            public InputStream call() {
                                return fis;
                            }
                        }, new Func1<InputStream, Observable<byte[]>>() {

                            @Override
                            public Observable<byte[]> call(InputStream t1) {
                                return Bytes.from(fis, maxBytesPerEmission)
                                        // move marker
                                        .doOnNext(new Action1<byte[]>() {
                                    @Override
                                    public void call(byte[] bytes) {
                                        currentPosition.addAndGet(bytes.length);
                                    }
                                });
                            }
                        }, new Action1<InputStream>() {
                            @Override
                            public void call(InputStream is) {
                                try {
                                    is.close();
                                } catch (IOException e) {
                                    // don't care
                                }
                            }
                        });
                    } catch (IOException e) {
                        return Observable.error(e);
                    }
                } else
                    return Observable.empty();
            }

        };
    }

}
