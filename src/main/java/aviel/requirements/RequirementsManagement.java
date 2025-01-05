package aviel.requirements;

public interface RequirementsManagement {
    ForSubscriber mangeSubscriber();

    interface ForSubscriber {
        void onItemUsed();

        void onComplete();

        long getRequestsGoal();
    }
}
