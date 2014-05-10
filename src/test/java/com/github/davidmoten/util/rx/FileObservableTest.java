package com.github.davidmoten.util.rx;

import static java.nio.file.StandardWatchEventKinds.ENTRY_CREATE;
import static java.nio.file.StandardWatchEventKinds.ENTRY_MODIFY;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.file.StandardWatchEventKinds;
import java.nio.file.WatchEvent;
import java.nio.file.WatchEvent.Kind;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.Test;
import org.mockito.InOrder;
import org.mockito.Mockito;

import com.github.davidmoten.rx.FileObservable;

import rx.Observable;
import rx.Observer;
import rx.Subscription;
import rx.schedulers.Schedulers;

public class FileObservableTest {

    @Test
    public void testNoEventsThrownIfFileDoesNotExist() throws InterruptedException {
        File file = new File("target/does-not-exist");
        Observable<WatchEvent<?>> events = FileObservable.from(file, ENTRY_MODIFY);
        final CountDownLatch latch = new CountDownLatch(1);
        Subscription sub = events.subscribeOn(Schedulers.io()).subscribe(new Observer<WatchEvent<?>>() {

            @Override
            public void onCompleted() {
                latch.countDown();
            }

            @Override
            public void onError(Throwable e) {
                latch.countDown();
                e.printStackTrace();
            }

            @Override
            public void onNext(WatchEvent<?> arg0) {
                latch.countDown();
            }
        });
        assertFalse(latch.await(100, TimeUnit.MILLISECONDS));
        sub.unsubscribe();
    }

    @Test
    public void testCreateAndModifyEventsForANonDirectoryFile() throws InterruptedException, IOException {
        File file = new File("target/f");
        file.delete();
        Observable<WatchEvent<?>> events = FileObservable.from(file, ENTRY_CREATE, ENTRY_MODIFY);
        final CountDownLatch latch = new CountDownLatch(1);
        @SuppressWarnings("unchecked")
        final List<Kind<?>> eventKinds = Mockito.mock(List.class);
        InOrder inOrder = Mockito.inOrder(eventKinds);
        final AtomicInteger errorCount = new AtomicInteger(0);
        Subscription sub = events.subscribeOn(Schedulers.io()).subscribe(new Observer<WatchEvent<?>>() {

            @Override
            public void onCompleted() {
                System.out.println("completed");
            }

            @Override
            public void onError(Throwable e) {
                errorCount.incrementAndGet();
            }

            @Override
            public void onNext(WatchEvent<?> event) {
                System.out.println("event=" + event);
                eventKinds.add(event.kind());
                latch.countDown();
            }
        });
        // sleep long enough for WatchService to start
        Thread.sleep(1000);
        file.createNewFile();
        FileOutputStream fos = new FileOutputStream(file, true);
        fos.write("hello there".getBytes());
        fos.close();
        // give the WatchService time to register the change
        Thread.sleep(100);
        assertTrue(latch.await(30000, TimeUnit.MILLISECONDS));
        inOrder.verify(eventKinds).add(StandardWatchEventKinds.ENTRY_CREATE);
        inOrder.verify(eventKinds).add(StandardWatchEventKinds.ENTRY_MODIFY);
        inOrder.verifyNoMoreInteractions();
        sub.unsubscribe();
        Thread.sleep(100);
        assertEquals(0, errorCount.get());
    }
}
