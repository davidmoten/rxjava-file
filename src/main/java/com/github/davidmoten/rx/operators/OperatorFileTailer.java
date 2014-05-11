package com.github.davidmoten.rx.operators;

import static com.github.davidmoten.rx.StringObservable2.lines;
import static com.github.davidmoten.rx.StringObservable2.trimEmpty;

import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.concurrent.atomic.AtomicLong;

import rx.Observable;
import rx.Observable.Operator;
import rx.Subscriber;
import rx.functions.Action1;
import rx.functions.Func1;
import rx.observers.Subscribers;
import rx.subjects.PublishSubject;

public class OperatorFileTailer implements Operator<String, Object> {

    private final File file;
    private final AtomicLong currentPosition = new AtomicLong();

    public static enum Event {
        FILE_EVENT;
    }

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
                if (file.length() > currentPosition.get()) {
                    return trimEmpty(lines(createReader(file, currentPosition.get())))
                    // as each line produced increment the current
                    // position with its length plus one for the new
                    // line separator
                            .doOnNext(moveCurrentPositionByStringLengthPlusOne(currentPosition));
                } else
                    return Observable.empty();
            }
        };
    }

    private static Action1<String> moveCurrentPositionByStringLengthPlusOne(final AtomicLong currentPosition) {
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
}
