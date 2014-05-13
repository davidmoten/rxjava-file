package com.github.davidmoten.rx;

import java.io.File;
import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardWatchEventKinds;
import java.nio.file.WatchEvent;
import java.nio.file.WatchEvent.Kind;
import java.nio.file.WatchService;
import java.util.concurrent.TimeUnit;

import rx.Observable;
import rx.Observable.OnSubscribe;
import rx.Subscriber;
import rx.functions.Func1;
import rx.observables.StringObservable;

import com.github.davidmoten.rx.operators.OperatorFileTailer;
import com.github.davidmoten.rx.operators.OperatorWatchServiceEvents;

/**
 * Observable utility methods related to {@link File}.
 */
public final class FileObservable {

    /**
     * Returns an {@link Observable} that uses NIO WatchService (and a dedicated
     * thread) to push modify events to an observable that reads and reports new
     * sequences of bytes to a subscriber. The NIO WatchService events are
     * sampled according to <code>sampleTimeMs</code> so that lots of discrete
     * activity on a file (for example a log file with very frequent entries)
     * does not prompt an inordinate number of file reads to pick up changes.
     * 
     * @param file
     *            the file to tail
     * @param startPosition
     *            start tailing file at position in bytes
     * @param sampleTimeMs
     *            sample time in millis
     * @return
     */
    public final static Observable<byte[]> tailFile(File file, long startPosition, long sampleTimeMs) {
        return from(file, StandardWatchEventKinds.ENTRY_CREATE, StandardWatchEventKinds.ENTRY_MODIFY,
                StandardWatchEventKinds.OVERFLOW)
        // don't care about the event details, just that there is one
                .cast(Object.class)
                // get lines once on subscription so we tail the lines
                // in the
                // file at startup
                .startWith(new Object())
                // emit a max of 1 event per sample period
                .sample(sampleTimeMs, TimeUnit.MILLISECONDS)
                // tail file triggered by events
                .lift(new OperatorFileTailer(file, startPosition));
    }

    /**
     * Returns an {@link Observable} that uses NIO WatchService (and a dedicated
     * thread) to push modify events to an observable that reads and reports new
     * lines to a subscriber. The NIO WatchService events are sampled according
     * to <code>sampleTimeMs</code> so that lots of discrete activity on a file
     * (for example a log file with very frequent entries) does not prompt an
     * inordinate number of file reads to pick up changes.
     * 
     * @param file
     *            the file to tail
     * @param startPosition
     *            start tailing file at position in bytes
     * @param sampleTimeMs
     *            sample time in millis
     * @param charset
     *            the character set to use to decode the bytes to a string
     * @return
     */
    public final static Observable<String> tailTextFile(File file, long startPosition, long sampleTimeMs,
            Charset charset) {
        return StringObservable.split(StringObservable.decode(tailFile(file, startPosition, sampleTimeMs), charset),
                "\n");
    }

    /**
     * Returns an {@link Observable} of {@link WatchEvent}s from a
     * {@link WatchService}.
     * 
     * @param watchService
     *            {@link WatchService} to generate events for
     * @return
     */
    public final static Observable<WatchEvent<?>> from(WatchService watchService) {
        return Observable.from(watchService).lift(new OperatorWatchServiceEvents());
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
     * @return
     */
    @SafeVarargs
    public final static Observable<WatchEvent<?>> from(final File file, Kind<?>... kinds) {
        return watchService(file, kinds).flatMap(TO_WATCH_EVENTS).filter(onlyRelatedTo(file));
    }

    /**
     * Creates a {@link WatchService} on subscribe for the given file and event
     * kinds.
     * 
     * @param file
     *            the file to watch
     * @param kinds
     *            event kinds to watch for
     * @return
     */
    @SafeVarargs
    public final static Observable<WatchService> watchService(final File file, final Kind<?>... kinds) {
        return Observable.create(new OnSubscribe<WatchService>() {

            @Override
            public void call(Subscriber<? super WatchService> subscriber) {
                final Path path = getBasePath(file);
                try {
                    WatchService watchService = path.getFileSystem().newWatchService();
                    path.register(watchService, kinds);
                    subscriber.onNext(watchService);
                    subscriber.onCompleted();
                } catch (IOException e) {
                    subscriber.onError(e);
                }
            }

        });
    }

    private final static Path getBasePath(final File file) {
        final Path path;
        if (file.exists() && file.isDirectory())
            path = Paths.get(file.toURI());
        else
            path = Paths.get(file.getParentFile().toURI());
        return path;
    }

    /**
     * Returns true if and only if the path corresponding to a WatchEvent
     * represents the given file. This will be the case for Create, Modify,
     * Delete events.
     * 
     * @param file
     *            the file to restrict events to
     * @return
     */
    private final static Func1<WatchEvent<?>, Boolean> onlyRelatedTo(final File file) {
        return new Func1<WatchEvent<?>, Boolean>() {

            @Override
            public Boolean call(WatchEvent<?> event) {

                final boolean ok;
                if (file.isDirectory())
                    ok = true;
                else if (StandardWatchEventKinds.OVERFLOW.equals(event.kind()))
                    // TODO allow overflow events through?
                    ok = true;
                else {
                    Object context = event.context();
                    if (context != null && context instanceof Path) {
                        Path p = (Path) context;
                        Path basePath = getBasePath(file);
                        File pFile = new File(basePath.toFile(), p.toString());
                        ok = pFile.getAbsolutePath().equals(file.getAbsolutePath());
                    } else
                        ok = false;
                }
                return ok;
            }
        };
    }

    private final static Func1<WatchService, Observable<WatchEvent<?>>> TO_WATCH_EVENTS = new Func1<WatchService, Observable<WatchEvent<?>>>() {

        @Override
        public Observable<WatchEvent<?>> call(WatchService watchService) {
            return from(watchService);
        }
    };
}
