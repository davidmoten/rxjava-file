package com.github.davidmoten.rx.operators;

import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.Reader;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicLong;

import rx.Observable;
import rx.Observable.Operator;
import rx.Subscriber;
import rx.functions.Action1;
import rx.functions.Func1;
import rx.observables.StringObservable;
import rx.observers.Subscribers;
import rx.subjects.PublishSubject;

/**
 * Reacts to source events by emitting new lines written to a file since the
 * last source event.
 */
public class OperatorFileTailer implements Operator<String, Object> {

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
    public Subscriber<? super Object> call(Subscriber<? super String> subscriber) {
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

    private static Func1<Object, Observable<String>> reportNewLines(final File file, final AtomicLong currentPosition) {
        return new Func1<Object, Observable<String>>() {
            @Override
            public Observable<String> call(Object event) {
                long length = file.length();
                if (length > currentPosition.get()) {
                    return trimEmpty(lines(createReader(file, currentPosition.get())))
                    // as each line produced increment the current
                    // position with its length plus one for the new
                    // line separator
                            .doOnNext(moveCurrentPositionByStringLengthPlusOneExceptForFirst(currentPosition));
                } else {
                    // file has shrunk in size, reset the current position to
                    // detect when it grows next
                    currentPosition.set(length);
                    return Observable.empty();
                }
            }
        };
    }

    private static Action1<String> moveCurrentPositionByStringLengthPlusOneExceptForFirst(
            final AtomicLong currentPosition) {
        return new Action1<String>() {
            boolean firstTime = true;

            @Override
            public void call(String line) {
                if (firstTime)
                    currentPosition.addAndGet(line.length());
                else
                    currentPosition.addAndGet(line.length() + 1);
                firstTime = false;
            }
        };
    }

    private static FileReader createReader(File file, long position) {
        try {
            FileReader reader = new FileReader(file);
            reader.skip(position);
            return reader;
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public static Observable<String> lines(Reader reader) {
        return StringObservable.split(StringObservable.from(reader), "\\n");
    }

    static Observable<String> trimEmpty(Observable<String> source) {
        final String terminator = UUID.randomUUID().toString() + UUID.randomUUID().toString();
        return Observable
        // end with
                .just(terminator)
                // start with
                .startWith(source)
                // ignore empty string at start
                .filter(ignoreEmptyStringAtStart())
                // buffer with a window of 2 step 1
                .buffer(2, 1)
                // do not emit element before terminator if empty string
                .concatMap(new Func1<List<String>, Observable<String>>() {
                    @Override
                    public Observable<String> call(List<String> list) {
                        if (list.size() > 1)
                            if (terminator.equals(list.get(1)))
                                if (list.get(0).length() == 0)
                                    return Observable.empty();
                                else
                                    return Observable.just(list.get(0));
                            else
                                return Observable.just(list.get(0));
                        else
                            // must be just the terminator
                            return Observable.empty();
                    }
                });
    }

    private static Func1<String, Boolean> ignoreEmptyStringAtStart() {
        return new Func1<String, Boolean>() {
            boolean firstTime = true;

            @Override
            public Boolean call(String s) {
                boolean result;
                if (firstTime)
                    result = s == null || s.length() > 0;
                else
                    result = true;
                firstTime = false;
                return result;
            }
        };
    }

}
