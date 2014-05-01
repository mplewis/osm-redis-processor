package osmproc.structure;

import java.util.*;

public class Tabulator<T> {

    private Map<T, Integer> counts = new HashMap<T, Integer>();

    public void addKey(T key) {
        if (counts.containsKey(key)) {
            counts.put(key, counts.get(key) + 1);
        } else {
            counts.put(key, 1);
        }
    }

    public List<TabCount<T>> getSortedCountsAsc() {
        List<TabCount<T>> sortedCounts = new ArrayList<TabCount<T>>();
        for (T key : counts.keySet()) {
            int val = counts.get(key);
            sortedCounts.add(new TabCount<T>(key, val));
        }
        Collections.sort(sortedCounts);
        return sortedCounts;
    }

    public List<TabCount<T>> getSortedCountsDesc() {
        List<TabCount<T>> sorted = getSortedCountsAsc();
        Collections.reverse(sorted);
        return sorted;
    }

    public Map<T, Integer> getCounts() {
        return counts;
    }

    public int uniqueKeyCount() {
        return counts.size();
    }

}
