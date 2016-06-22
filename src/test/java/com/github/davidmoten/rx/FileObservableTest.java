package com.github.davidmoten.rx;

import static java.nio.file.StandardWatchEventKinds.ENTRY_CREATE;
import static java.nio.file.StandardWatchEventKinds.ENTRY_MODIFY;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.nio.charset.Charset;
import java.nio.file.StandardWatchEventKinds;
import java.nio.file.WatchEvent;
import java.nio.file.WatchEvent.Kind;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.Test;
import org.mockito.InOrder;
import org.mockito.Mockito;

import rx.Observable;
import rx.Observer;
import rx.Subscription;
import rx.functions.Action0;
import rx.functions.Action1;
import rx.observers.TestSubscriber;
import rx.schedulers.Schedulers;

public class FileObservableTest {

    @Test
    public void testNoEventsThrownIfFileDoesNotExist() throws InterruptedException {
        File file = new File("target/does-not-exist");
        Observable<WatchEvent<?>> events = FileObservable.from(file, ENTRY_MODIFY);
        final CountDownLatch latch = new CountDownLatch(1);
        Subscription sub = events.subscribeOn(Schedulers.io())
                .subscribe(new Observer<WatchEvent<?>>() {

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
    public void testCreateAndModifyEventsForANonDirectoryFile()
            throws InterruptedException, IOException {
        File file = new File("target/f");
        file.delete();
        Observable<WatchEvent<?>> events = FileObservable.from(file).kind(ENTRY_MODIFY)
                .kind(ENTRY_CREATE).pollInterval(100, TimeUnit.MILLISECONDS)
                .pollDuration(100, TimeUnit.MILLISECONDS).events();
        final CountDownLatch latch = new CountDownLatch(1);
        @SuppressWarnings("unchecked")
        final List<Kind<?>> eventKinds = Mockito.mock(List.class);
        InOrder inOrder = Mockito.inOrder(eventKinds);
        final AtomicInteger errorCount = new AtomicInteger(0);
        Subscription sub = events.subscribeOn(Schedulers.io())
                .subscribe(new Observer<WatchEvent<?>>() {

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

    @Test
    public void testFileTailingFromStartOfFile() throws InterruptedException, IOException {
        final File log = new File("target/test.log");
        log.delete();
        log.createNewFile();
        append(log, "a0");

        Observable<String> tailer = FileObservable.tailer().file(log).onWatchStarted(new Action0() {
            @Override
            public void call() {
                append(log, "a1");
                append(log, "a2");
            }
        }).sampleTimeMs(50).utf8().tailText();
        final List<String> list = new ArrayList<String>();
        final CountDownLatch latch = new CountDownLatch(3);
        Subscription sub = tailer.subscribeOn(Schedulers.io()).subscribe(new Action1<String>() {
            @Override
            public void call(String line) {
                System.out.println("received: '" + line + "'");
                list.add(line);
                latch.countDown();
            }
        });
        assertTrue(latch.await(10, TimeUnit.SECONDS));
        assertEquals(Arrays.asList("a0", "a1", "a2"), list);
        sub.unsubscribe();
    }

    @Test
    public void testFileTailingWhenFileIsCreatedAfterSubscription()
            throws InterruptedException, IOException {
        final File log = new File("target/test.log");
        log.delete();

        append(log, "a0");
        Observable<String> tailer = FileObservable.tailer().file(log).startPosition(0)
                .sampleTimeMs(50).utf8().onWatchStarted(new Action0() {
                    @Override
                    public void call() {
                        try {
                            log.createNewFile();
                        } catch (IOException e) {
                            throw new RuntimeException(e);
                        }
                        append(log, "a1");
                        append(log, "a2");
                    }
                }).tailText();

        final List<String> list = new ArrayList<String>();
        final CountDownLatch latch = new CountDownLatch(3);
        Subscription sub = tailer.subscribeOn(Schedulers.io()).subscribe(new Action1<String>() {
            @Override
            public void call(String line) {
                System.out.println("received: '" + line + "'");
                list.add(line);
                latch.countDown();
            }
        });
        assertTrue(latch.await(10, TimeUnit.SECONDS));
        assertEquals(Arrays.asList("a0", "a1", "a2"), list);
        sub.unsubscribe();
    }

    private static void append(File file, String line) {
        try {
            FileOutputStream fos = new FileOutputStream(file, true);
            fos.write(line.getBytes(Charset.forName("UTF-8")));
            fos.write('\n');
            fos.close();
        } catch (FileNotFoundException e) {
            throw new RuntimeException(e);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Test
    public void testTailTextFileStreamsFromEndOfFileIfSpecified()
            throws FileNotFoundException, InterruptedException {
        File file = new File("target/test1.txt");
        file.delete();
        try (PrintStream out = new PrintStream(file)) {
            out.println("line 1");
        }
        final List<String> list = new ArrayList<String>();
        TestSubscriber<String> ts = TestSubscriber.create();
        FileObservable.tailer().file(file).startPosition(file.length()).sampleTimeMs(10).utf8()
                .tailText()
                // for each
                .doOnNext(new Action1<String>() {

                    @Override
                    public void call(String line) {
                        System.out.println(line);
                        list.add(line);
                    }
                }).subscribeOn(Schedulers.newThread()).subscribe(ts);
        Thread.sleep(1100);
        assertTrue(list.isEmpty());
        try (PrintStream out = new PrintStream(new FileOutputStream(file, true))) {
            out.println("line 2");
        }
        Thread.sleep(1100);
        assertEquals(1, list.size());
        assertEquals("line 2", list.get(0).trim());
        ts.unsubscribe();
    }

    @Test
    public void testTailTextFileStreamsFromEndOfFileIfDeleteOccurs()
            throws InterruptedException, IOException {
        File file = new File("target/test2.txt");
        file.delete();
        try (PrintStream out = new PrintStream(file)) {
            out.println("line 1");
        }
        final List<String> list = new ArrayList<String>();
        Subscription sub = FileObservable.tailer().file(file).startPosition(file.length())
                .sampleTimeMs(10).utf8().tailText()
                // for each
                .doOnNext(new Action1<String>() {

                    @Override
                    public void call(String line) {
                        System.out.println(line);
                        list.add(line);
                    }
                }).subscribeOn(Schedulers.newThread()).subscribe();
        // delay must be long enough for last update timestamp to change on
        // windows (resolution to the second)
        Thread.sleep(1100);
        assertTrue(list.isEmpty());
        // delete file then make it bigger than it was
        assertTrue(file.delete());
        try (PrintStream out = new PrintStream(new FileOutputStream(file, true))) {
            out.println("line 2");
            out.println("line 3");
        }
        Thread.sleep(1100);
        assertEquals(2, list.size());
        assertEquals("line 2", list.get(0).trim());
        assertEquals("line 3", list.get(1).trim());
        sub.unsubscribe();
    }
}
