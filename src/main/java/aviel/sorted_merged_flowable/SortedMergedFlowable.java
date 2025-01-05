package aviel.sorted_merged_flowable;

import aviel.requirements.RequirementsManagement;
import io.reactivex.rxjava3.core.Flowable;
import org.reactivestreams.Subscriber;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

public class SortedMergedFlowable<T> extends Flowable<T> {
    private final Comparator<T> comparator;
    private final Set<MainSortMergedSubscription<T>> subscriptions;
    private final List<Flowable<T>> flowables;
    private final RequirementsManagement requirementsManagement;

    public SortedMergedFlowable(Comparator<T> comparator,
                                RequirementsManagement requirementsManagement) {
        this.comparator = comparator;
        subscriptions = Collections.newSetFromMap(new ConcurrentHashMap<>());
        flowables = new LinkedList<>();
        this.requirementsManagement = requirementsManagement;
    }

    @Override
    protected void subscribeActual(Subscriber<? super T> subscriber) {
        MainSortMergedSubscription<T> subscription = new MainSortMergedSubscription<>(comparator,
                                                                                      subscriber,
                                                                                      subscriptions::remove,
                                                                                      requirementsManagement);
        subscriptions.add(subscription);
        for (Flowable<T> flowable : flowables) {
            subscription.subscribeWith(flowable);
        }
        subscriber.onSubscribe(subscription);
    }

    public void add(Flowable<T> flowable) {
        flowables.add(flowable);
        for (MainSortMergedSubscription<T> subscription : subscriptions) {
            subscription.subscribeWith(flowable);
        }
    }
}
