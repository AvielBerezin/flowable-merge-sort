package aviel;

import java.util.Comparator;
import java.util.concurrent.atomic.AtomicLong;

public class UniqueRef<Item> {
    private static final AtomicLong counter = new AtomicLong();
    private final Item item;
    protected final long id;

    public UniqueRef(Item item) {
        this.item = item;
        id = counter.getAndIncrement();
    }

    public final Item get() {
        return item;
    }

    @Override
    public final String toString() {
        return "Unique[%d](%s)".formatted(id, item.toString());
    }

    @Override
    public final boolean equals(Object obj) {
        return obj instanceof UniqueRef<?> uniqueRef && item.equals(uniqueRef.item);
    }

    public static <Item> Comparator<UniqueRef<Item>> comparing(Comparator<Item> comparator) {
        return Comparator.<UniqueRef<Item>, Item>comparing(UniqueRef::get, comparator)
                         .thenComparingLong(ref -> ref.id);
    }

    public static <Item> UniqueRef<Item> of(Item item) {
        return new UniqueRef<>(item);
    }

    public static <Item extends java.lang.Comparable<Item>> Comparable<Item> of(Item item) {
        return new Comparable<>(item);
    }

    public static class Comparable<Item extends java.lang.Comparable<Item>> extends UniqueRef<Item> implements java.lang.Comparable<UniqueRef<Item>> {
        public Comparable(Item item) {
            super(item);
        }

        @Override
        public final int compareTo(UniqueRef<Item> boxedItem) {
            int comparing = get().compareTo(boxedItem.get());
            return comparing == 0 ? Long.signum(id - boxedItem.id) : comparing;
        }
    }
}
