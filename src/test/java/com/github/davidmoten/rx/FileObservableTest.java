package com.github.davidmoten.rx;

import static java.nio.file.StandardWatchEventKinds.ENTRY_CREATE;
import static java.nio.file.StandardWatchEventKinds.ENTRY_MODIFY;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.hamcrest.Matchers.equalTo;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.FileSystem;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.nio.file.StandardWatchEventKinds;
import java.nio.file.WatchEvent;
import java.nio.file.WatchEvent.Kind;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import org.awaitility.Awaitility;
import org.awaitility.core.ConditionFactory;
import org.junit.Before;
import org.junit.Test;
import org.mockito.InOrder;
import org.mockito.Mockito;

import com.google.common.jimfs.Configuration;
import com.google.common.jimfs.Jimfs;
import com.google.common.jimfs.WatchServiceConfiguration;

import rx.Observable;
import rx.Observer;
import rx.Subscription;
import rx.functions.Action0;
import rx.functions.Action1;
import rx.observers.TestSubscriber;
import rx.schedulers.Schedulers;


public class FileObservableTest {

    private FileSystem fileSystem;

    @Before
	public void setup() throws IOException {
		WatchServiceConfiguration watchServiceConfig = WatchServiceConfiguration.polling(10, MILLISECONDS);
		Configuration fileSystemConfig = Configuration.osX().toBuilder().setWatchServiceConfiguration(watchServiceConfig)
				.build();
		this.fileSystem = Jimfs.newFileSystem(fileSystemConfig);
		Path target = fileSystem.getPath("target");
		Files.createDirectories(target);
	}

    @Test
    public void testNoEventsThrownIfFileDoesNotExist() throws InterruptedException {
        Path file = fileSystem.getPath("target", "does-not-exist");
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
    public void testCreateAndModifyEventsForANonDirectoryFileBlockForever()
            throws InterruptedException, IOException {
    	Path file = fileSystem.getPath("target", "f");
        Observable<WatchEvent<?>> events = FileObservable.from(file).kind(ENTRY_MODIFY)
                .kind(ENTRY_CREATE).events();
                        
        checkCreateAndModifyEvents(file, events);
    }

    @Test
    public void testCreateAndModifyEventsForANonDirectoryFilePollEveryInterval()
            throws InterruptedException, IOException {
    	Path file = fileSystem.getPath("target", "f");
        Observable<WatchEvent<?>> events = FileObservable.from(file).kind(ENTRY_MODIFY)
                .kind(ENTRY_CREATE).pollInterval(100, TimeUnit.MILLISECONDS).events();
        
        checkCreateAndModifyEvents(file, events);
    }

    @Test
    public void testCreateAndModifyEventsForANonDirectoryFileBlockingPollEveryInterval()
            throws InterruptedException, IOException {
    	Path file = fileSystem.getPath("target", "f");
        Observable<WatchEvent<?>> events = FileObservable.from(file).kind(ENTRY_MODIFY)
                .kind(ENTRY_CREATE).pollInterval(100, TimeUnit.MILLISECONDS)
                .pollDuration(100, TimeUnit.MILLISECONDS).events();
        
        checkCreateAndModifyEvents(file, events);
    }

    private void checkCreateAndModifyEvents(Path file, Observable<WatchEvent<?>> events)
            throws InterruptedException, IOException, FileNotFoundException {
    	Files.deleteIfExists(file);
        
        @SuppressWarnings("unchecked")
        final List<Kind<?>> eventKinds = Mockito.mock(List.class);
        InOrder inOrder = Mockito.inOrder(eventKinds);
        
        final AtomicBoolean isComplete = new AtomicBoolean();
        final AtomicInteger eventCount = new AtomicInteger();
        final AtomicInteger errorCount = new AtomicInteger();
        
        Subscription sub = events.subscribeOn(Schedulers.io())
                .subscribe(new Observer<WatchEvent<?>>() {

                    @Override
                    public void onCompleted() {
                    	isComplete.set(true);
                    }

                    @Override
                    public void onError(Throwable e) {
                        errorCount.incrementAndGet();
                    }

                    @Override
                    public void onNext(WatchEvent<?> event) {
                        eventKinds.add(event.kind());
                        eventCount.incrementAndGet();
                    }
                });

        sleepForWatchEvent();
        
        Files.createFile(file);
        await().untilAtomic(eventCount, equalTo(1));
        
        Files.write(file, "hello there".getBytes(),StandardOpenOption.CREATE, StandardOpenOption.APPEND);
        await().untilAtomic(eventCount, equalTo(2));
                
        inOrder.verify(eventKinds).add(StandardWatchEventKinds.ENTRY_CREATE);
        inOrder.verify(eventKinds).add(StandardWatchEventKinds.ENTRY_MODIFY);
        inOrder.verifyNoMoreInteractions();

        sub.unsubscribe();        
        assertEquals(0, errorCount.get());
    }

	private ConditionFactory await() {
		return Awaitility.await().with().pollInterval(10, MILLISECONDS).atMost(200, MILLISECONDS);
	}

	private void sleepForWatchEvent() throws InterruptedException {
		Thread.sleep(20);
	}

    @Test
    public void testFileTailingFromStartOfFile() throws InterruptedException, IOException {
    	final Path log = fileSystem.getPath("target", "test.log");
    	Files.deleteIfExists(log);
    	Files.createFile(log);
        append(log, "a0");

        Observable<String> tailer = FileObservable.tailer().file(log).onWatchStarted(new Action0() {
            @Override
            public void call() {
                append(log, "a1");
                append(log, "a2");
            }
        }).sampleTimeMs(50).utf8().tailText();
        
        final List<String> list = new ArrayList<>();
        final AtomicInteger eventCount = new AtomicInteger();
        
        Subscription sub = tailer.subscribeOn(Schedulers.io()).subscribe(new Action1<String>() {
            @Override
            public void call(String line) {
                list.add(line);
                eventCount.incrementAndGet();
            }
        });
                
        await().untilAtomic(eventCount, equalTo(3));
        assertEquals(Arrays.asList("a0", "a1", "a2"), list);
        
        sub.unsubscribe();
    }
    
    @Test
    public void testFileTailingWhenFileIsCreatedAfterSubscription()
            throws InterruptedException, IOException {
    	final Path log = fileSystem.getPath("target", "test.log");
    	Files.deleteIfExists(log);
        append(log, "a0");
        
		Observable<String> tailer = FileObservable.tailer().file(log).startPosition(0).sampleTimeMs(50).utf8()
				.onWatchStarted(new Action0() {
					@Override
					public void call() {
						try {
							if (!Files.exists(log)) {
								Files.createFile(log);
							}
						} catch (IOException e) {
							throw new RuntimeException(e);
						}
						append(log, "a1");
						append(log, "a2");
					}
				}).tailText();

        final List<String> list = new ArrayList<String>();
        final AtomicInteger eventCount = new AtomicInteger();
        
        Subscription sub = tailer.subscribeOn(Schedulers.io()).subscribe(new Action1<String>() {
            @Override
            public void call(String line) {
                list.add(line);
                eventCount.incrementAndGet();
            }
        });
        
        await().untilAtomic(eventCount, equalTo(3));
        assertEquals(Arrays.asList("a0", "a1", "a2"), list);
        
        sub.unsubscribe();
    }
   
    @Test
    public void testTailTextFileStreamsFromEndOfFileIfSpecified()
            throws InterruptedException, IOException {   	        
        Path file = fileSystem.getPath("target", "test1.txt");
    	Files.deleteIfExists(file);
    	append(file, "line 1");
        
        final List<String> list = new ArrayList<>();
        TestSubscriber<String> ts = TestSubscriber.create();
        final AtomicInteger eventCount = new AtomicInteger();
        
        FileObservable.tailer().file(file).startPosition(Files.size(file)).sampleTimeMs(10).utf8()
                .tailText()
                // for each
                .doOnNext(new Action1<String>() {
                    @Override
                    public void call(String line) {
                        list.add(line);
                        eventCount.incrementAndGet();
                    }
                }).subscribeOn(Schedulers.newThread()).subscribe(ts);
        
        sleepForWatchEvent();
        
        assertTrue(list.isEmpty());
        
        append(file, "line 2");
        await().untilAtomic(eventCount, equalTo(1));
        assertEquals(1, list.size());
        assertEquals("line 2", list.get(0).trim());
        
        ts.unsubscribe();
    }

    @Test
    public void testTailTextFileStreamsFromEndOfFileIfDeleteOccurs()
            throws InterruptedException, IOException {
    	Path file = fileSystem.getPath("target", "test2.txt");
    	Files.deleteIfExists(file);
    	append(file, "hello there");
    	
        final List<String> list = new ArrayList<String>();
        final AtomicInteger eventCount = new AtomicInteger();
        
        Subscription sub = FileObservable.tailer().file(file).startPosition(Files.size(file))
                .sampleTimeMs(10).utf8().tailText()
                // for each
                .doOnNext(new Action1<String>() {
                    @Override
                    public void call(String line) {
                        list.add(line);
                        eventCount.incrementAndGet();
                    }
                }).subscribeOn(Schedulers.newThread()).subscribe();

        sleepForWatchEvent();
        
        assertTrue(list.isEmpty());
        
        // delete file then make it bigger than it was
        assertTrue(Files.deleteIfExists(file));
        sleepForWatchEvent();        
                
        append(file, "line 2");
        await().untilAtomic(eventCount, equalTo(1));
        
        append(file, "line 3");
        await().untilAtomic(eventCount, equalTo(2));
                        
        assertEquals(2, list.size());
        assertEquals("line 2", list.get(0).trim());
        assertEquals("line 3", list.get(1).trim());
        
        sub.unsubscribe();
    }
    
    private static void append(Path file, String line) {
    	try {
			Files.write(file, (line + "\n").getBytes(StandardCharsets.UTF_8), StandardOpenOption.CREATE, StandardOpenOption.APPEND, StandardOpenOption.WRITE);
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
    }
}
