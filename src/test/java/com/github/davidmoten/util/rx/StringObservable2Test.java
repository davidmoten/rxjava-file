package com.github.davidmoten.util.rx;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.Arrays;
import java.util.List;

import org.junit.Test;

import rx.Observable;

public class StringObservable2Test {

    @Test
    public void testTrimEmptyDoesNothingIfNonZeroLengthAtEnd() {
        List<String> list = StringObservable2.trimEmpty(Observable.from("a", "b")).toList().toBlockingObservable()
                .single();
        assertEquals(Arrays.asList("a", "b"), list);
    }

    @Test
    public void testTrimEmptyIgnoresLastIfZeroLengthAtEnd() {
        List<String> list = StringObservable2.trimEmpty(Observable.from("a", "b", "")).toList().toBlockingObservable()
                .single();
        assertEquals(Arrays.asList("a", "b"), list);
    }

    @Test
    public void testTrimEmptyDoesNothingEmptySource() {
        List<String> list = StringObservable2.trimEmpty(Observable.<String> empty()).toList().toBlockingObservable()
                .single();
        assertTrue(list.isEmpty());
    }

}
