package com.github.davidmoten.util.rx;

import java.io.Reader;
import java.util.List;
import java.util.UUID;

import rx.Observable;
import rx.functions.Func1;
import rx.observables.StringObservable;

public class StringObservable2 {

    public static Observable<String> lines(Reader reader) {
        return StringObservable.split(StringObservable.from(reader), "\\n");
    }

    public static Observable<String> trimEmpty(Observable<String> source) {
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
