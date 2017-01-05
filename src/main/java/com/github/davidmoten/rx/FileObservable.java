package com.github.davidmoten.rx;

import java.io.File;
import java.nio.charset.Charset;
import java.nio.file.FileSystem;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardWatchEventKinds;
import java.nio.file.WatchEvent;
import java.nio.file.WatchEvent.Kind;
import java.nio.file.WatchService;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import com.github.davidmoten.guavamini.Optional;
import com.github.davidmoten.guavamini.Preconditions;
import com.github.davidmoten.rx.internal.operators.OnSubscribeWatchServiceEvents;
import com.github.davidmoten.rx.internal.operators.OperatorFileTailer;
import com.github.davidmoten.rx.util.BackpressureStrategy;

import rx.Observable;
import rx.Scheduler;
import rx.functions.Action0;
import rx.functions.Action1;
import rx.functions.Func0;
import rx.functions.Func1;
import rx.observables.GroupedObservable;

/**
 * Observable utility methods related to {@link File}.
 */
public final class FileObservable {

    public static final int DEFAULT_MAX_BYTES_PER_EMISSION = 8192;

    private FileObservable() {
        // prevent instantiation
    }

    /**
     * @see {@link #tailFile(Path, long, long, int)}
     */
    public final static Observable<byte[]> tailFile(File file, long startPosition,
            long sampleTimeMs, int chunkSize) {
        return tailFile(file.toPath(), startPosition, sampleTimeMs, chunkSize);
    }
    
    /**
     * Returns an {@link Observable} that uses NIO {@link WatchService} (and a
     * dedicated thread) to push modified events to an observable that reads and
     * reports new sequences of bytes to a subscriber. The NIO
     * {@link WatchService} events are sampled according to
     * <code>sampleTimeMs</code> so that lots of discrete activity on a file
     * (for example a log file with very frequent entries) does not prompt an
     * inordinate number of file reads to pick up changes.
     * 
     * @param file
     *            the file to tail
     * @param startPosition
     *            start tailing file at position in bytes
     * @param sampleTimeMs
     *            sample time in millis
     * @param chunkSize
     *            max array size of each element emitted by the Observable. Is
     *            also used as the buffer size for reading from the file. Try
     *            {@link FileObservable#DEFAULT_MAX_BYTES_PER_EMISSION} if you
     *            don't know what to put here.
     * @return observable of byte arrays
     */
    public final static Observable<byte[]> tailFile(Path file, long startPosition,
            long sampleTimeMs, int chunkSize) {
        Preconditions.checkNotNull(file);
        Observable<Object> events = from(file, StandardWatchEventKinds.ENTRY_CREATE,
                StandardWatchEventKinds.ENTRY_MODIFY, StandardWatchEventKinds.OVERFLOW)
                        // don't care about the event details, just that there
                        // is one
                        .cast(Object.class)
                        // get lines once on subscription so we tail the lines
                        // in the file at startup
                        .startWith(new Object());
        return tailFile(file, startPosition, sampleTimeMs, chunkSize, events);
    }

    /**
     * @see {@link #tailFile(Path, long, long, int, Observable)
     */
    public final static Observable<byte[]> tailFile(File file, long startPosition,
            long sampleTimeMs, int chunkSize, Observable<?> events) {
        return tailFile(file.toPath(), startPosition, sampleTimeMs, chunkSize, events);
    }
    
    /**
     * Returns an {@link Observable} that uses given given observable to push
     * modified events to an observable that reads and reports new sequences of
     * bytes to a subscriber. The NIO {@link WatchService} MODIFY and OVERFLOW
     * events are sampled according to <code>sampleTimeMs</code> so that lots of
     * discrete activity on a file (for example a log file with very frequent
     * entries) does not prompt an inordinate number of file reads to pick up
     * changes. File create events are not sampled and are always passed
     * through.
     * 
     * @param file
     *            the file to tail
     * @param startPosition
     *            start tailing file at position in bytes
     * @param sampleTimeMs
     *            sample time in millis for MODIFY and OVERFLOW events
     * @param chunkSize
     *            max array size of each element emitted by the Observable. Is
     *            also used as the buffer size for reading from the file. Try
     *            {@link FileObservable#DEFAULT_MAX_BYTES_PER_EMISSION} if you
     *            don't know what to put here.
     * @param events
     *            trigger a check for file changes. Use
     *            {@link Observable#interval(long, TimeUnit)} for example.
     * @return observable of byte arrays
     */
    public final static Observable<byte[]> tailFile(Path file, long startPosition,
            long sampleTimeMs, int chunkSize, Observable<?> events) {
        Preconditions.checkNotNull(file);
        return sampleModifyOrOverflowEventsOnly(events, sampleTimeMs)
                // tail file triggered by events
                .lift(new OperatorFileTailer(file, startPosition, chunkSize));
    }

    /**
     * @see {@link #tailTextFile(Path, long, long, Charset)}
     */
    public final static Observable<String> tailTextFile(File file, long startPosition,
            long sampleTimeMs, Charset charset) {
        return tailTextFile(file.toPath(), startPosition, sampleTimeMs, charset);
    }
    
    /**
     * Returns an {@link Observable} that uses NIO {@link WatchService} (and a
     * dedicated thread) to push modified events to an observable that reads and
     * reports new lines to a subscriber. The NIO WatchService MODIFY and
     * OVERFLOW events are sampled according to <code>sampleTimeMs</code> so
     * that lots of discrete activity on a file (for example a log file with
     * very frequent entries) does not prompt an inordinate number of file reads
     * to pick up changes. File create events are not sampled and are always
     * passed through.
     * 
     * @param file
     *            the file to tail
     * @param startPosition
     *            start tailing file at position in bytes
     * @param sampleTimeMs
     *            sample time in millis for MODIFY and OVERFLOW events
     * @param charset
     *            the character set to use to decode the bytes to a string
     * @return observable of strings
     */
    public final static Observable<String> tailTextFile(Path file, long startPosition,
            long sampleTimeMs, Charset charset) {
        return toLines(tailFile(file, startPosition, sampleTimeMs, DEFAULT_MAX_BYTES_PER_EMISSION),
                charset);
    }

    /**
     * @see {@link #tailTextFile(Path, long, int, Charset, Observable)}
     */
    public final static Observable<String> tailTextFile(File file, long startPosition,
            int chunkSize, Charset charset, Observable<?> events) {
        return tailTextFile(file.toPath(), startPosition, chunkSize, charset, events);
    }
    
    /**
     * Returns an {@link Observable} of String that uses the given events stream
     * to trigger checks on file change so that new lines can be read and
     * emitted.
     * 
     * @param file
     *            the file to tail, cannot be null
     * @param startPosition
     *            start tailing file at position in bytes
     * @param chunkSize
     *            max array size of each element emitted by the Observable. Is
     *            also used as the buffer size for reading from the file. Try
     *            {@link FileObservable#DEFAULT_MAX_BYTES_PER_EMISSION} if you
     *            don't know what to put here.
     * @param charset
     *            the character set to use to decode the bytes to a string
     * @param events
     *            trigger a check for file changes. Use
     *            {@link Observable#interval(long, TimeUnit)} for example.
     * @return observable of strings
     */
    public final static Observable<String> tailTextFile(Path file, long startPosition,
            int chunkSize, Charset charset, Observable<?> events) {
        Preconditions.checkNotNull(file);
        Preconditions.checkNotNull(charset);
        Preconditions.checkNotNull(events);
        return toLines(events.lift(new OperatorFileTailer(file, startPosition, chunkSize))
                .onBackpressureBuffer(), charset);
    }

    /**
     * Returns an {@link Observable} of {@link WatchEvent}s from a
     * {@link WatchService}.
     * 
     * @param watchService
     *            WatchService to generate events from
     * @param scheduler
     *            schedules polls of the watchService
     * @param pollDuration
     *            duration of each poll
     * @param pollDurationUnit
     *            time unit for the duration of each poll
     * @param pollInterval
     *            interval between polls of the watchService
     * @param pollIntervalUnit
     *            time unit for the interval between polls
     * @param backpressureStrategy
     *            backpressures strategy to apply
     * @return an observable of WatchEvents from watchService
     */
    public final static Observable<WatchEvent<?>> from(WatchService watchService,
            Scheduler scheduler, long pollDuration, TimeUnit pollDurationUnit, long pollInterval,
            TimeUnit pollIntervalUnit, BackpressureStrategy backpressureStrategy) {
        Preconditions.checkNotNull(watchService);
        Preconditions.checkNotNull(scheduler);
        Preconditions.checkNotNull(pollDurationUnit);
        Preconditions.checkNotNull(backpressureStrategy);
        Observable<WatchEvent<?>> o = Observable
                .create(new OnSubscribeWatchServiceEvents(watchService, scheduler, pollDuration,
                        pollDurationUnit, pollInterval, pollIntervalUnit));
        if (backpressureStrategy == BackpressureStrategy.BUFFER) {
            return o.onBackpressureBuffer();
        } else if (backpressureStrategy == BackpressureStrategy.DROP)
            return o.onBackpressureDrop();
        else if (backpressureStrategy == BackpressureStrategy.LATEST)
            return o.onBackpressureLatest();
        else
            throw new RuntimeException("unrecognized backpressureStrategy " + backpressureStrategy);
    }

    /**
     * Returns an {@link Observable} of {@link WatchEvent}s from a
     * {@link WatchService}.
     * 
     * @param watchService
     *            {@link WatchService} to generate events for
     * @return observable of watch events from the watch service
     */
    public final static Observable<WatchEvent<?>> from(WatchService watchService) {
        return from(watchService, rx.schedulers.Schedulers.trampoline(), Long.MAX_VALUE,
                TimeUnit.MILLISECONDS, 0, TimeUnit.SECONDS, BackpressureStrategy.BUFFER);
    }

    
    /**
     * @see {@link #from(Path, Kind...)}
     */
    @SafeVarargs
    public final static Observable<WatchEvent<?>> from(final File file, Kind<?>... kinds) {
        return from(file.toPath(), kinds);
    }
    
    /**
     * If file does not exist at subscribe time then is assumed to not be a
     * directory. If the file is not a directory (bearing in mind the aforesaid
     * assumption) then a {@link WatchService} is set up on its parent and
     * {@link WatchEvent}s of the given kinds are filtered to concern the file
     * in question. If the file is a directory then a {@link WatchService} is
     * set up on the directory and all events are passed through of the given
     * kinds.
     * 
     * @param file
     *            file to watch
     * @param kinds
     *            event kinds to watch for and emit
     * @return observable of watch events
     */
    public final static Observable<WatchEvent<?>> from(final Path file, Kind<?>... kinds) {
        return from(file, null, kinds);
    }

    /**
     * @see {@link #from(Path, List)}
     */
    public final static Observable<WatchEvent<?>> from(final File file, List<Kind<?>> kinds) {
        return from(file.toPath(), kinds);
    }
    
    /**
     * If file does not exist at subscribe time then is assumed to not be a
     * directory. If the file is not a directory (bearing in mind the aforesaid
     * assumption) then a {@link WatchService} is set up on its parent and
     * {@link WatchEvent}s of the given kinds are filtered to concern the file
     * in question. If the file is a directory then a {@link WatchService} is
     * set up on the directory and all events are passed through of the given
     * kinds.
     * 
     * @param file
     *            file to watch
     * @param kinds
     *            event kinds to watch for and emit
     * @return observable of watch events
     */
    public final static Observable<WatchEvent<?>> from(final Path file, List<Kind<?>> kinds) {
        return from(file, null, kinds.toArray(new Kind<?>[] {}));
    }

    /**
     * @see {@link #from(Path, Action0, Kind...)}
     */
    public final static Observable<WatchEvent<?>> from(final File file,
            final Action0 onWatchStarted, Kind<?>... kinds) {
        return from(file.toPath(), onWatchStarted, kinds);
    }
    
    /**
     * If file does not exist at subscribe time then is assumed to not be a
     * directory. If the file is not a directory (bearing in mind the aforesaid
     * assumption) then a {@link WatchService} is set up on its parent and
     * {@link WatchEvent}s of the given kinds are filtered to concern the file
     * in question. If the file is a directory then a {@link WatchService} is
     * set up on the directory and all events are passed through of the given
     * kinds.
     * 
     * @param file
     *            file to generate watch events from
     * @param onWatchStarted
     *            called when WatchService is created
     * @param kinds
     *            kinds of watch events to register for
     * @return observable of watch events
     */
    public final static Observable<WatchEvent<?>> from(final Path file,
            final Action0 onWatchStarted, Kind<?>... kinds) {
        return watchService(file, kinds)
                // when watch service created call onWatchStarted
                .doOnNext(new Action1<WatchService>() {
                    @Override
                    public void call(WatchService w) {
                        if (onWatchStarted != null)
                            onWatchStarted.call();
                    }
                })
                // emit events from the WatchService
                .flatMap(TO_WATCH_EVENTS)
                // restrict to events related to the file
                .filter(onlyRelatedTo(file));
    }

    /**
     * @see {@link #watchService(Path, Kind...)}
     */
    @SafeVarargs
    public final static Observable<WatchService> watchService(final File file,
            final Kind<?>... kinds) {
        return watchService(file.toPath(), kinds);

    }
    
    /**
     * Creates a {@link WatchService} on subscribe for the given file and event
     * kinds.
     * 
     * @param file
     *            the file to watch
     * @param kinds
     *            event kinds to watch for
     * @return observable of watch events
     */
    public final static Observable<WatchService> watchService(final Path file,
            final Kind<?>... kinds) {
        return Observable.defer(new Func0<Observable<WatchService>>() {

            @Override
            public Observable<WatchService> call() {
                try {
                    final Path basePath = getBasePath(file);
                    WatchService watchService = basePath.getFileSystem().newWatchService();
                    basePath.register(watchService, kinds);
                    return Observable.just(watchService);
                } catch (Exception e) {
                    return Observable.error(e);
                }
            }
        });

    }
    
    private final static Path getBasePath(final Path path) {
    	return Files.exists(path) && Files.isDirectory(path)
    			? path
    			: path.getParent();
    }

    /**
     * Returns true if and only if the path corresponding to a WatchEvent
     * represents the given file. This will be the case for Create, Modify,
     * Delete events.
     * 
     * @param file
     *            the file to restrict events to
     * @return predicate
     */
    private final static Func1<WatchEvent<?>, Boolean> onlyRelatedTo(final Path path) {
        return new Func1<WatchEvent<?>, Boolean>() {

            @Override
            public Boolean call(WatchEvent<?> event) {

                final boolean ok;
                if (Files.isDirectory(path))
                    ok = true;
                else if (StandardWatchEventKinds.OVERFLOW.equals(event.kind()))
                    ok = true;
                else {
                    Object context = event.context();
                    if (context != null && context instanceof Path) {
                        Path p = (Path) context;
                        Path basePath = getBasePath(path);
                        Path pPath = basePath.resolve(p);
                        ok = pPath.toAbsolutePath().equals(path.toAbsolutePath());
                    } else
                        ok = false;
                }
                return ok;
            }
        };
    }

    private static Observable<String> toLines(Observable<byte[]> bytes, Charset charset) {
        return Strings.split(Strings.decode(bytes, charset), "\n");
    }

    private final static Func1<WatchService, Observable<WatchEvent<?>>> TO_WATCH_EVENTS = new Func1<WatchService, Observable<WatchEvent<?>>>() {

        @Override
        public Observable<WatchEvent<?>> call(WatchService watchService) {
            return from(watchService);
        }
    };

    private static Observable<Object> sampleModifyOrOverflowEventsOnly(Observable<?> events,
            final long sampleTimeMs) {
        return events
                // group by true if is modify or overflow, false otherwise
                .groupBy(IS_MODIFY_OR_OVERFLOW)
                // only sample if is modify or overflow
                .flatMap(sampleIfTrue(sampleTimeMs));
    }

    private static Func1<GroupedObservable<Boolean, ?>, Observable<?>> sampleIfTrue(
            final long sampleTimeMs) {
        return new Func1<GroupedObservable<Boolean, ?>, Observable<?>>() {

            @Override
            public Observable<?> call(GroupedObservable<Boolean, ?> group) {
                // if is modify or overflow WatchEvent
                if (group.getKey())
                    return group.sample(sampleTimeMs, TimeUnit.MILLISECONDS);
                else
                    return group;
            }
        };
    }

    private static Func1<Object, Boolean> IS_MODIFY_OR_OVERFLOW = new Func1<Object, Boolean>() {

        @Override
        public Boolean call(Object event) {
            if (event instanceof WatchEvent) {
                WatchEvent<?> w = (WatchEvent<?>) event;
                String kind = w.kind().name();
                if (kind.equals(StandardWatchEventKinds.ENTRY_MODIFY.name())
                        || kind.equals(StandardWatchEventKinds.OVERFLOW.name())) {
                    return true;
                } else
                    return false;
            } else
                return false;
        }
    };

    public static WatchEventsBuilder from(File file) {
        return from(file.toPath());
    }
    
    public static WatchEventsBuilder from(Path path) {
        return new WatchEventsBuilder(path);
    }

    public static final class WatchEventsBuilder {
        private final Path path;
        private Optional<Scheduler> scheduler = Optional.absent();
        private long pollInterval = 0;
        private TimeUnit pollIntervalUnit = TimeUnit.MILLISECONDS;
        private Optional<Long> pollDuration = Optional.absent();
        private TimeUnit pollDurationUnit = TimeUnit.MILLISECONDS;
        private final List<Kind<?>> kinds = new ArrayList<>();
        private BackpressureStrategy backpressureStrategy = BackpressureStrategy.BUFFER;
        private Action0 onWatchStarted = new Action0() {
            @Override
            public void call() {
                // do nothing
            }
        };

        private WatchEventsBuilder(Path path) {
            this.path = path;
        }
        
        public WatchEventsBuilder onWatchStarted(Action0 onWatchStarted) {
            this.onWatchStarted = onWatchStarted;
            return this;
        }

        public WatchEventsBuilder scheduler(Scheduler scheduler) {
            this.scheduler = Optional.of(scheduler);
            return this;
        }

        public WatchEventsBuilder pollInterval(long interval, TimeUnit unit) {
            this.pollInterval = interval;
            this.pollIntervalUnit = unit;
            if (!pollDuration.isPresent())
                this.pollDuration = Optional.of(0L);
            return this;
        }

        public WatchEventsBuilder pollDuration(long duration, TimeUnit unit) {
            this.pollDuration = Optional.of(duration);
            this.pollDurationUnit = unit;
            return this;
        }

        public WatchEventsBuilder kind(Kind<?> kind) {
            this.kinds.add(kind);
            return this;
        }

        public WatchEventsBuilder kinds(Kind<?>... kinds) {
            for (Kind<?> kind : kinds) {
                this.kinds.add(kind);
            }
            return this;
        }

        public WatchEventsBuilder onBackpressure(BackpressureStrategy strategy) {
            this.backpressureStrategy = strategy;
            return this;
        }

        public Observable<WatchEvent<?>> events() {
            return watchService(path, kinds.toArray(new Kind<?>[] {}))
            		// when watch service created call onWatchStarted
                    .doOnNext(new Action1<WatchService>() {
                        @Override
                        public void call(WatchService w) {
                            if (onWatchStarted != null)
                                onWatchStarted.call();
                        }
                    })
                    .flatMap(new Func1<WatchService, Observable<WatchEvent<?>>>() {
                        @Override
                        public Observable<WatchEvent<?>> call(WatchService watchService) {
                            if (!scheduler.isPresent()) {
                                if (!pollDuration.isPresent() || pollDuration.get() == 0) {
                                    scheduler = Optional.of(rx.schedulers.Schedulers.computation());
                                } else {
                                    // poll will block so don't do on
                                    // computation()
                                    scheduler = Optional.of(rx.schedulers.Schedulers.io());
                                }
                            }
                            return from(watchService, scheduler.get(),
                                    pollDuration.or(Long.MAX_VALUE), pollDurationUnit, pollInterval,
                                    pollIntervalUnit, backpressureStrategy);
                        }
                    });
        }

    }

    public static TailerBuilder tailer() {
        return new TailerBuilder();
    }

    public static final class TailerBuilder {

        private Path path = null;
        private long startPosition = 0;
        private long sampleTimeMs = 500;
        private int chunkSize = 8192;
        private Charset charset = Charset.defaultCharset();
        private Observable<?> source = null;
        private Action0 onWatchStarted = new Action0() {
            @Override
            public void call() {
                // do nothing
            }
        };
		private FileSystem fileSystem = FileSystems.getDefault();

        private TailerBuilder() {
        }

        /**
         * @see {@link #file(Path)}
         */
        public TailerBuilder file(File file) {
            return file(file.toPath());
        }
        
        /**
         * The file to tail.
         * 
         * @param file
         *            file to tail
         * @return the builder (this)
         */
        public TailerBuilder file(Path file) {
            this.path = file;
            return this;
        }

        public TailerBuilder file(String filename) {
        	return file(fileSystem.getPath(filename));
        }

        public TailerBuilder onWatchStarted(Action0 onWatchStarted) {
            this.onWatchStarted = onWatchStarted;
            return this;
        }

        /**
         * The startPosition in bytes in the file to commence the tail from. 0 =
         * start of file. Defaults to 0.
         * 
         * @param startPosition
         *            start position
         * @return this
         */
        public TailerBuilder startPosition(long startPosition) {
            this.startPosition = startPosition;
            return this;
        }

        /**
         * Specifies sampling to apply to the source observable (which could be
         * very busy if a lot of writes are occurring for example). Sampling is
         * only applied to file updates (MODIFY and OVERFLOW), file creation
         * events are always passed through. File deletion events are ignored
         * (in fact are not requested of NIO).
         * 
         * @param sampleTimeMs
         *            sample time in ms
         * @return this
         */
        public TailerBuilder sampleTimeMs(long sampleTimeMs) {
            this.sampleTimeMs = sampleTimeMs;
            return this;
        }

        /**
         * Emissions from the tailed file will be no bigger than this.
         * 
         * @param chunkSize
         *            chunk size in bytes
         * @return this
         */
        public TailerBuilder chunkSize(int chunkSize) {
            this.chunkSize = chunkSize;
            return this;
        }

        /**
         * The charset of the file. Only used for tailing a text file.
         * 
         * @param charset
         *            charset to decode with
         * @return this
         */
        public TailerBuilder charset(Charset charset) {
            this.charset = charset;
            return this;
        }

        /**
         * The charset of the file. Only used for tailing a text file.
         * 
         * @param charset
         *            charset to decode the file with
         * @return this
         */
        public TailerBuilder charset(String charset) {
            return charset(Charset.forName(charset));
        }

        public TailerBuilder utf8() {
            return charset("UTF-8");
        }

        public TailerBuilder source(Observable<?> source) {
            this.source = source;
            return this;
        }

        public Observable<byte[]> tail() {

            return tailFile(path, startPosition, sampleTimeMs, chunkSize, getSource());
        }

        public Observable<String> tailText() {
            return tailTextFile(path, startPosition, chunkSize, charset, getSource());
        }

        private Observable<?> getSource() {
            if (source == null)
                return from(path, onWatchStarted, StandardWatchEventKinds.ENTRY_CREATE,
                        StandardWatchEventKinds.ENTRY_MODIFY, StandardWatchEventKinds.OVERFLOW);
            else
                return source;

        }

    }

}
