package aviel.sorted_merged_flowable;

import aviel.UniqueRef;
import aviel.requirements.RequirementsManagement;
import org.apache.logging.log4j.LogManager;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;

import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

public class SortedMergedSubscriber<T> implements Subscriber<T> {
    private static final AtomicInteger count = new AtomicInteger(0);
    private final int id = count.getAndIncrement();
    private final MainSortMergedSubscription<T> mainSubscription;
    private final RequirementsManagement.ForSubscriber subscribersRequirementsManagement;
    private Subscription subscription;
    private final Set<UniqueRef<T>> sortedSet;
    private long pendingItems;

    SortedMergedSubscriber(MainSortMergedSubscription<T> mainSubscription,
                           RequirementsManagement.ForSubscriber subscribersRequirementsManagement) {
        this.mainSubscription = mainSubscription;
        sortedSet = new HashSet<>();
        pendingItems = 0L;
        this.subscribersRequirementsManagement = subscribersRequirementsManagement;
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

    public void onItemUsed() {
        subscribersRequirementsManagement.onItemUsed();
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
        subscribersRequirementsManagement.onComplete();
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

    public void ensureRequest() {
        long goal = subscribersRequirementsManagement.getRequestsGoal();
        long pendingSize = this.pendingSize();
        if (pendingSize < goal) {
            LogManager.getLogger(Integer.toString(id))
                      .info("requesting {} for total of {}", goal - pendingSize, goal);
            subscription.request(goal - pendingSize);
            pendingItems += goal - pendingSize;
        }
    }

    private long pendingSize() {
        return this.sortedSet.size() + this.pendingItems;
    }

    @Override
    public String toString() {
        return Integer.toString(id);
    }
}
