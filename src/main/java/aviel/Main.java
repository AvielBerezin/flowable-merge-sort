package aviel;

import aviel.requirements.ConstRequirementsManagement;
import aviel.sorted_merged_flowable.SortedMergedFlowable;
import io.reactivex.rxjava3.core.BackpressureOverflowStrategy;
import io.reactivex.rxjava3.core.Flowable;
import io.reactivex.rxjava3.disposables.Disposable;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Comparator;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

public class Main {
    public static void main(String[] args) throws InterruptedException {
        Flowable.just("hello world").subscribe(System.out::println, System.err::println).dispose();

        SortedMergedFlowable<Integer> mergedInts = new SortedMergedFlowable<Integer>(Comparator.naturalOrder(),
//                                                                                     new BoundedTrafficBasedRequirementsManagement(10L));
                                                                                     new ConstRequirementsManagement(1L));
        mergedInts.add(Flowable.intervalRange(0, 8, 0, 50, TimeUnit.MILLISECONDS)
                               .onBackpressureBuffer(1, () -> LogManager.getLogger("buffer").info("overflew"), BackpressureOverflowStrategy.DROP_OLDEST)
                               .map(Math::toIntExact).map(x -> x * 3)
                               .doOnNext(item -> loggerOf(0).info("next {}", item))
                               .doOnComplete(() -> loggerOf(0).info("completed"))
                               .doOnError(throwable -> loggerOf(0).error("erred", throwable)));
        mergedInts.add(Flowable.intervalRange(0, 6, 0, 75, TimeUnit.MILLISECONDS)
                               .onBackpressureBuffer(1, () -> LogManager.getLogger("buffer").info("overflew"), BackpressureOverflowStrategy.DROP_OLDEST)
                               .map(Math::toIntExact).map(x -> x * 3 + 1)
                               .doOnNext(item -> loggerOf(1).info("next {}", item))
                               .doOnComplete(() -> loggerOf(1).info("completed"))
                               .doOnError(throwable -> loggerOf(1).error("erred", throwable)));
        mergedInts.add(Flowable.intervalRange(0, 4, 0, 100, TimeUnit.MILLISECONDS)
                               .onBackpressureBuffer(1, () -> LogManager.getLogger("buffer").info("overflew"), BackpressureOverflowStrategy.DROP_OLDEST)
                               .map(Math::toIntExact).map(x -> x * 3 + 2)
                               .doOnNext(item -> loggerOf(2).info("next {}", item))
                               .doOnComplete(() -> loggerOf(2).info("completed"))
                               .doOnError(throwable -> loggerOf(2).error("erred", throwable)));

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

    public static Logger loggerOf(int id) {
        return LogManager.getLogger(Integer.toString(id));
    }
}
