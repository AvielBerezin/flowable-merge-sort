package aviel.requirements;

import java.util.HashSet;
import java.util.Set;

public class BoundedTrafficBasedRequirementsManagement implements RequirementsManagement {
    private double total;
    private final long bound;
    private final double memoryCoefficient;
    private final Set<BoundedTrafficBasedSubscribersRequirementsManagement> subscribersRequirementsManagements;

    public BoundedTrafficBasedRequirementsManagement(long bound) {
        if (bound < 1) {
            throw new IllegalArgumentException("bound cannot be less then 1: " + bound);
        }
        this.total = 0D;
        this.bound = bound;
        memoryCoefficient = 0.95D;
        subscribersRequirementsManagements = new HashSet<>();
    }

    @Override
    public synchronized ForSubscriber mangeSubscriber() {
        if (subscribersRequirementsManagements.size() >= bound) {
            throw new IllegalStateException("subscribers amount is surpassing the bound " + bound);
        }
        BoundedTrafficBasedSubscribersRequirementsManagement subscribersRequirementsManagement = new BoundedTrafficBasedSubscribersRequirementsManagement();
        subscribersRequirementsManagements.add(subscribersRequirementsManagement);
        return subscribersRequirementsManagement;
    }

    private class BoundedTrafficBasedSubscribersRequirementsManagement implements ForSubscriber {
        private double items = 0;

        @Override
        public void onItemUsed() {
            synchronized (BoundedTrafficBasedRequirementsManagement.this) {
                items = items * memoryCoefficient + 1D;
                for (BoundedTrafficBasedSubscribersRequirementsManagement subscribersRequirementsManagement : subscribersRequirementsManagements) {
                    subscribersRequirementsManagement.items *= memoryCoefficient;
                }
                total = total * memoryCoefficient + 1D;
            }
        }

        @Override
        public void onComplete() {
            synchronized (BoundedTrafficBasedRequirementsManagement.this) {
                subscribersRequirementsManagements.remove(this);
                total -= items;
            }
        }

        @Override
        public long getRequestsGoal() {
            synchronized (BoundedTrafficBasedRequirementsManagement.this) {
                if (total == 0) {
                    return Math.max(1, bound / subscribersRequirementsManagements.size());
                }
                return Math.max(1L, (long) (items / total * bound));
            }
        }
    }
}
