package aviel.sorted_merged_flowable;

import aviel.UniqueRef;
import aviel.requirements.RequirementsManagement;
import io.reactivex.rxjava3.core.Flowable;
import org.apache.logging.log4j.LogManager;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;

import java.util.*;
import java.util.function.Consumer;

public class MainSortMergedSubscription<T> implements Subscription {
    private final Set<SortedMergedSubscriber<T>> subscribersAll;
    private final Set<SortedMergedSubscriber<T>> subscribersWithNotDataRightNow;
    private final Set<SortedMergedSubscriber<T>> subscribersWithoutRequests;

    private final SortedSet<Entry> mainSortedSetBuffer;
    private final List<Throwable> subscribersErrors;
    private final Subscriber<? super T> mainSubscriber;
    private final Consumer<MainSortMergedSubscription<T>> onTermination;
    private final RequirementsManagement requirementsManagement;
    private long mainPendingItems;
    boolean canceled;
    boolean terminated;

    public MainSortMergedSubscription(Comparator<T> comparator,
                                      Subscriber<? super T> mainSubscriber,
                                      Consumer<MainSortMergedSubscription<T>> onTermination,
                                      RequirementsManagement requirementsManagement) {
        mainSortedSetBuffer = new TreeSet<>(Comparator.comparing(Entry::item, UniqueRef.comparing(comparator)));
        this.mainSubscriber = mainSubscriber;
        this.requirementsManagement = requirementsManagement;
        mainPendingItems = 0;
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
        SortedMergedSubscriber<T> subscriber = new SortedMergedSubscriber<>(this, requirementsManagement.mangeSubscriber());
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
            first.source.onItemUsed();
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
