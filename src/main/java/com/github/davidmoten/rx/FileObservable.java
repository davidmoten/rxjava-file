package com.github.davidmoten.rx;

import java.io.File;
import java.io.IOException;
import java.nio.file.ClosedWatchServiceException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardWatchEventKinds;
import java.nio.file.WatchEvent;
import java.nio.file.WatchEvent.Kind;
import java.nio.file.WatchKey;
import java.nio.file.WatchService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import rx.Observable;
import rx.Observable.OnSubscribe;
import rx.Subscriber;
import rx.Subscription;
import rx.functions.Func1;
import rx.operators.OperatorConcat;

import com.github.davidmoten.rx.operators.OperatorFileTailer;

/**
 * Observable utility methods related to {@link File}.
 */
public final class FileObservable {

    /**
     * Returns an {@link Observable} that uses NIO WatchService (and a dedicated
     * thread) to push modify events to an observable that reads and reports new
     * lines to a subscriber. The NIO WatchService events are sampled according
     * to <code>sampleTimeMs</code> so that lots of discrete activity on a file
     * (for example a log file with very frequent entries) does not prompt an
     * inordinate number of file reads to pick up changes.
     * 
     * @param file
     * @param startPosition
     * @param sampleTimeMs
     * @return
     */
    public final static Observable<String> tailFile(File file, long startPosition, long sampleTimeMs) {
        return from(file, StandardWatchEventKinds.ENTRY_CREATE, StandardWatchEventKinds.ENTRY_MODIFY,
                StandardWatchEventKinds.OVERFLOW)
        // don't care about the event details, just that there is one
                .cast(Object.class)
                // get lines once on subscription so we tail the lines in the
                // file at startup
                .startWith(new Object())
                // emit a max of 1 event per sample period
                .sample(sampleTimeMs, TimeUnit.MILLISECONDS)
                // tail file triggered by events
                .lift(new OperatorFileTailer(file, startPosition));
    }

    /**
     * Returns an {@link Observable} of {@link WatchEvent}s from a
     * {@link WatchService}.
     * 
     * @param watchService
     * @return
     */
    public final static Observable<WatchEvent<?>> from(WatchService watchService) {
        return Observable.create(new WatchServiceOnSubscribe(watchService));
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
     * @param kinds
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
     * @param kinds
     * @return
     */
    @SafeVarargs
    public final static Observable<WatchService> watchService(final File file, final Kind<?>... kinds) {
        return Observable.create(new OnSubscribe<WatchService>() {

            @Override
            public void call(Subscriber<? super WatchService> subscriber) {
                OperatorConcat c;
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

    static class WatchServiceOnSubscribe implements OnSubscribe<WatchEvent<?>> {

        private final WatchService watchService;
        private final AtomicBoolean subscribed = new AtomicBoolean(true);

        WatchServiceOnSubscribe(WatchService watchService) {
            this.watchService = watchService;
        }

        @Override
        public void call(Subscriber<? super WatchEvent<?>> subscriber) {

            if (!subscribed.get()) {
                subscriber.onError(new RuntimeException(
                        "WatchService closed. You can only subscribe once to a WatchService."));
                return;
            }
            subscriber.add(createSubscriptionToCloseWatchService(watchService, subscribed, subscriber));

            emitEvents(subscriber);
        }

        private void emitEvents(Subscriber<? super WatchEvent<?>> subscriber) {
            // get the first event before looping
            WatchKey key = nextKey(subscriber);

            while (key != null) {
                if (subscriber.isUnsubscribed())
                    return;
                // we have a polled event, now we traverse it and
                // receive all the states from it
                for (WatchEvent<?> event : key.pollEvents()) {
                    if (subscriber.isUnsubscribed())
                        return;
                    else
                        subscriber.onNext(event);
                }

                boolean valid = key.reset();
                if (!valid && subscribed.get()) {
                    subscriber.onCompleted();
                    return;
                } else if (!valid)
                    return;

                key = nextKey(subscriber);
            }
        }

        private WatchKey nextKey(Subscriber<? super WatchEvent<?>> subscriber) {
            try {
                // this command blocks but unsubscribe close the watch
                // service and interrupt it
                return watchService.take();
            } catch (ClosedWatchServiceException e) {
                // must have unsubscribed
                if (subscribed.get())
                    subscriber.onCompleted();
                return null;
            } catch (InterruptedException e) {
                // this case is problematic because unsubscribe may call
                // Thread.interrupt() before calling the unsubscribe method of
                // the Subscription. Thus at this point we don't know if a
                // deliberate interrupt was called in which case I would call
                // onComplete or if unsubscribe was called in which case I
                // should not call anything. For the moment I choose to not call
                // anything partly because a deliberate stop of the
                // watchService.take ignorant of the Observable should ideally
                // happen via a call to the WatchService.close() method rather
                // than Thread.interrupt().
                // TODO raise the issue with RxJava team in particular
                // Subscriptions.from(Future) calling FutureTask.cancel(true)
                try {
                    watchService.close();
                } catch (IOException e1) {
                    // do nothing
                }
                return null;
            }
        }

    }

    private final static Subscription createSubscriptionToCloseWatchService(final WatchService watchService,
            final AtomicBoolean subscribed, final Subscriber<? super WatchEvent<?>> subscriber) {
        return new Subscription() {

            @Override
            public void unsubscribe() {
                try {
                    watchService.close();
                    subscribed.set(false);
                } catch (ClosedWatchServiceException e) {
                    // do nothing
                    subscribed.set(false);
                } catch (IOException e) {
                    // do nothing
                    subscribed.set(false);
                }
            }

            @Override
            public boolean isUnsubscribed() {
                return !subscribed.get();
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
