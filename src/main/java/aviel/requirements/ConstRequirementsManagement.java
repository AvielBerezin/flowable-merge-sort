package aviel.requirements;

public class ConstRequirementsManagement implements RequirementsManagement {
    private final long goal;

    public ConstRequirementsManagement(long goal) {
        if (goal < 1) {
            throw new IllegalArgumentException("goal cannot be less then 1: " + goal);
        }
        this.goal = goal;
    }

    @Override
    public ForSubscriber mangeSubscriber() {
        return new ForSubscriber() {
            @Override
            public void onItemUsed() {
            }

            @Override
            public void onComplete() {
            }

            @Override
            public long getRequestsGoal() {
                return goal;
            }
        };
    }
}
