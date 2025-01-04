package aviel;

import io.reactivex.rxjava3.core.BackpressureOverflowStrategy;
import io.reactivex.rxjava3.core.Flowable;
import io.reactivex.rxjava3.disposables.Disposable;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;

public class Main {
    public static void main(String[] args) throws InterruptedException {
        Flowable.just("hello world").subscribe(System.out::println, System.err::println).dispose();

        SortedMergedFlowable<Integer> mergedInts = new SortedMergedFlowable<Integer>(Comparator.naturalOrder());
        Flowable<Integer> base =
                Flowable.intervalRange(0, 5, 0, 100, TimeUnit.MILLISECONDS)
                        .onBackpressureBuffer(1, () -> LogManager.getLogger("buffer").info("overflew"), BackpressureOverflowStrategy.DROP_OLDEST)
                        .map(Math::toIntExact);
//                Flowable.range(0, 5);
        mergedInts.add(base.map(x -> x * 3)
                           .doOnNext(item -> loggerOf(0).info("next {}", item))
                           .doOnComplete(() -> loggerOf(0).info("completed"))
                           .doOnError(throwable -> loggerOf(0).error("erred", throwable)));
        mergedInts.add(base.map(x -> x * 3 + 1)
                           .doOnNext(item -> loggerOf(1).info("next {}", item))
                           .doOnComplete(() -> loggerOf(1).info("completed"))
                           .doOnError(throwable -> loggerOf(1).error("erred", throwable)));
        mergedInts.add(base.map(x -> x * 3 + 2)
                           .doOnNext(item -> loggerOf(2).info("next {}", item))
                           .doOnComplete(() -> loggerOf(2).info("completed"))
                           .doOnError(throwable -> loggerOf(2).error("erred", throwable)));
        mergedInts.add(base.map(x -> x * 3)
                           .doOnNext(item -> loggerOf(3).info("next {}", item))
                           .doOnComplete(() -> loggerOf(3).info("completed"))
                           .doOnError(throwable -> loggerOf(3).error("erred", throwable)));

        Logger resultLogger = LogManager.getLogger("result");
        CountDownLatch latch = new CountDownLatch(1);
        Disposable mergeDisposer;
        mergeDisposer = mergedInts.doOnNext(message -> resultLogger.info("next {}", message))
                                  .doOnError(resultLogger::error)
                                  .doOnComplete(() -> resultLogger.info("done"))
                                  .onErrorComplete()
                                  .toList()
                                  .doOnSuccess(values -> resultLogger.info("final: {}", values))
                                  .doOnTerminate(latch::countDown)
                                  .subscribe();
        try {
            latch.await();
        } finally {
            mergeDisposer.dispose();
        }
    }

    private static Logger loggerOf(int id) {
        return LogManager.getLogger(Integer.toString(id));
    }

    public static class SortedMergedFlowable<T> extends Flowable<T> {
        private final Comparator<T> comparator;
        private final Set<MainSortMergedSubscription<T>> subscriptions;
        private final List<Flowable<T>> flowables;

        public SortedMergedFlowable(Comparator<T> comparator) {
            this.comparator = comparator;
            subscriptions = Collections.newSetFromMap(new ConcurrentHashMap<>());
            flowables = new LinkedList<>();
        }

        @Override
        protected void subscribeActual(Subscriber<? super T> subscriber) {
            MainSortMergedSubscription<T> subscription = new MainSortMergedSubscription<>(comparator, subscriber, subscriptions::remove);
            subscriptions.add(subscription);
            for (Flowable<T> flowable : flowables) {
                subscription.subscribeWith(flowable);
            }
            subscriber.onSubscribe(subscription);
        }

        private void add(Flowable<T> flowable) {
            flowables.add(flowable);
            for (MainSortMergedSubscription<T> subscription : subscriptions) {
                subscription.subscribeWith(flowable);
            }
        }
    }

    private static class MainSortMergedSubscription<T> implements Subscription {
        private final Set<SortedMergedSubscriber<T>> subscribersAll;
        private final Set<SortedMergedSubscriber<T>> subscribersWithNotDataRightNow;
        private final Set<SortedMergedSubscriber<T>> subscribersWithoutRequests;

        private final SortedSet<Entry> mainSortedSetBuffer;
        private final List<Throwable> subscribersErrors;
        private final Subscriber<? super T> mainSubscriber;
        private final Comparator<T> comparator;
        private final Consumer<MainSortMergedSubscription<T>> onTermination;
        private long mainPendingItems;
        boolean canceled;
        boolean terminated;

        public MainSortMergedSubscription(Comparator<T> comparator,
                                          Subscriber<? super T> mainSubscriber,
                                          Consumer<MainSortMergedSubscription<T>> onTermination) {
            mainSortedSetBuffer = new TreeSet<>(Comparator.comparing(Entry::item, UniqueRef.comparing(comparator)));
            this.mainSubscriber = mainSubscriber;
            mainPendingItems = 0;
            this.comparator = comparator;
            subscribersAll = new HashSet<>();
            subscribersWithNotDataRightNow = new HashSet<>();
            subscribersWithoutRequests = new HashSet<>();
            subscribersErrors = new LinkedList<>();
            this.onTermination = onTermination;
            canceled = false;
            terminated = false;
        }

        @Override
        public synchronized void request(long count) {
            mainPendingItems += count;
            for (SortedMergedSubscriber<T> subscriber : subscribersWithoutRequests) {
                subscriber.ensureRequest();
            }
            tryEmit();
        }

        @Override
        public synchronized void cancel() {
            if (!canceled) {
                for (SortedMergedSubscriber<T> subscriber : subscribersAll) {
                    subscriber.cancel();
                }
            }
            canceled = true;
        }

        public synchronized void subscribeWith(Flowable<T> flowable) {
            if (!terminated) {
                flowable.subscribe(this.addSubscriber());
            }
        }

        private SortedMergedSubscriber<T> addSubscriber() {
            SortedMergedSubscriber<T> subscriber = new SortedMergedSubscriber<>(this);
            subscribersAll.add(subscriber);
            subscribersWithNotDataRightNow.add(subscriber);
            subscribersWithoutRequests.add(subscriber);
            return subscriber;
        }

        public synchronized void onSubscriberNext(SortedMergedSubscriber<T> subscriber, UniqueRef<T> item) {
            LogManager.getLogger().debug("got next {} on {}", item, subscriber);
            mainSortedSetBuffer.add(new Entry(subscriber, item));
            subscribersWithNotDataRightNow.remove(subscriber);
            if (subscriber.pendingItems() <= 0L) {
                subscribersWithoutRequests.add(subscriber);
            }
            tryEmit();
        }

        public synchronized void onSubscriberComplete(SortedMergedSubscriber<T> subscriber) {
            subscribersAll.remove(subscriber);
            subscribersWithNotDataRightNow.remove(subscriber);
            subscribersWithoutRequests.remove(subscriber);
            tryTerminate();
        }

        public synchronized void onSubscriberError(SortedMergedSubscriber<T> subscriber, Throwable throwable) {
            subscribersAll.remove(subscriber);
            subscribersWithNotDataRightNow.remove(subscriber);
            subscribersWithoutRequests.remove(subscriber);
            subscribersErrors.add(throwable);
            tryTerminate();
        }

        private void tryTerminate() {
            if (subscribersAll.isEmpty()) {
                tryEmit();
                if (subscribersErrors.isEmpty()) {
                    mainSubscriber.onComplete();
                } else if (subscribersErrors.size() == 1) {
                    mainSubscriber.onError(new Exception("one of the subscribers erred", subscribersErrors.get(0)));
                } else {
                    Exception exception = new Exception(subscribersErrors.size() + " subscribers erred ");
                    subscribersErrors.forEach(exception::addSuppressed);
                    mainSubscriber.onError(exception);
                }
                terminated = true;
                onTermination.accept(this);
            }
        }

        private void tryEmit() {
            Set<SortedMergedSubscriber<T>> sources = new HashSet<>();
            while (mainPendingItems > 0 && subscribersWithNotDataRightNow.isEmpty()) {
                LogManager.getLogger("result").debug("mainSortedSetBuffer: {}", mainSortedSetBuffer);
                LogManager.getLogger("result").debug("subscribers: {}", subscribersAll);
                LogManager.getLogger("result").debug("subscribersPending: {}", subscribersWithNotDataRightNow);
                Entry first;
                try {
                    first = mainSortedSetBuffer.first();
                } catch (NoSuchElementException e) {
                    return;
                }
                mainSortedSetBuffer.remove(first);
                first.source().remove(first.item());
                sources.add(first.source());
                if (first.source().currentlyEmpty() && subscribersAll.contains(first.source())) {
                    subscribersWithNotDataRightNow.add(first.source());
                }
                mainSubscriber.onNext(first.item().get());
                mainPendingItems--;
            }
            if (mainPendingItems > 0) {
                for (SortedMergedSubscriber<T> source : sources) {
                    source.ensureRequest();
                }
            }
        }

        private class Entry {
            private final UniqueRef<T> item;
            private final SortedMergedSubscriber<T> source;

            private Entry(SortedMergedSubscriber<T> source, UniqueRef<T> item) {
                this.item = item;
                this.source = source;
            }

            public UniqueRef<T> item() {
                return item;
            }

            public SortedMergedSubscriber<T> source() {
                return source;
            }

            @Override
            public String toString() {
                return "[%s] %s".formatted(source.toString(), item.toString());
            }
        }
    }

    public static class SortedMergedSubscriber<T> implements Subscriber<T> {
        private static final AtomicInteger count = new AtomicInteger(0);
        private final int id = count.getAndIncrement();
        private final MainSortMergedSubscription<T> mainSubscription;
        private Subscription subscription;
        private final Set<UniqueRef<T>> sortedSet;
        private long pendingItems;

        private SortedMergedSubscriber(MainSortMergedSubscription<T> mainSubscription) {
            this.mainSubscription = mainSubscription;
            sortedSet = new HashSet<>();
            pendingItems = 0L;
        }

        @Override
        public void onSubscribe(Subscription subscription) {
            this.subscription = subscription;
        }

        @Override
        public void onNext(T item) {
            synchronized (mainSubscription) {
                UniqueRef<T> uniqueRef = UniqueRef.of(item);
                sortedSet.add(uniqueRef);
                pendingItems--;
                mainSubscription.onSubscriberNext(this, uniqueRef);
            }
        }

        @Override
        public void onError(Throwable throwable) {
            synchronized (mainSubscription) {
                mainSubscription.cancel();
                mainSubscription.onSubscriberError(this, throwable);
            }
        }

        @Override
        public void onComplete() {
            mainSubscription.onSubscriberComplete(this);
        }

        public void cancel() {
            synchronized (mainSubscription) {
                subscription.cancel();
            }
        }

        public boolean currentlyEmpty() {
            synchronized (mainSubscription) {
                return sortedSet.isEmpty();
            }
        }

        public void remove(UniqueRef<T> item) {
            synchronized (mainSubscription) {
                sortedSet.remove(item);
            }
        }

        public long pendingItems() {
            return pendingItems;
        }

        private void ensureRequest() {
            int goal = 2;
            long pendingSize = this.pendingSize();
            if (pendingSize < goal) {
                this.request(goal - pendingSize);
            }
        }

        private long pendingSize() {
            return this.sortedSet.size() + this.pendingItems;
        }

        private void request(long count) {
            loggerOf(id).info("requesting {}", count);
            subscription.request(count);
            pendingItems += count;
        }

        @Override
        public String toString() {
            return Integer.toString(id);
        }
    }
}
