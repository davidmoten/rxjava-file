package com.github.davidmoten.util.rx;

import java.util.List;
import java.util.concurrent.TimeUnit;

import org.junit.Test;
import org.mockito.InOrder;
import org.mockito.Mockito;

import rx.Subscription;
import rx.functions.Action1;
import rx.subjects.PublishSubject;

public class SampleTest {

    @Test
    public void testSampleDoesNotRepeatEmissions() throws InterruptedException {
        PublishSubject<Integer> subject = PublishSubject.create();
        @SuppressWarnings("unchecked")
        final List<Integer> list = Mockito.mock(List.class);
        Subscription sub = subject
        // sample
                .sample(100, TimeUnit.MILLISECONDS)
                // subscribe and record emissions
                .subscribe(new Action1<Integer>() {
                    @Override
                    public void call(Integer n) {
                        list.add(n);
                    }
                });
        // these items should be emitted before the first sample window closes
        subject.onNext(1);
        subject.onNext(2);
        subject.onNext(3);

        // wait for at least one more sample window to have passed
        Thread.sleep(250);

        // check that sample only reported the last item (3) once
        InOrder inOrder = Mockito.inOrder(list);
        inOrder.verify(list).add(3);
        inOrder.verifyNoMoreInteractions();

        sub.unsubscribe();
    }

}
