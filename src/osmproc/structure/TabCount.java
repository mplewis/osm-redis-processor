package osmproc.structure;

public class TabCount<T> implements Comparable<TabCount<T>> {
    public final T key;
    public final int count;

    public TabCount(T key, int count) {
        this.key = key;
        this.count = count;
    }

    @Override
    public int compareTo(TabCount<T> other) {
        assert other != null;
        return this.count - other.count;
    }
}
