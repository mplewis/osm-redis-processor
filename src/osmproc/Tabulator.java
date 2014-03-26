package osmproc;

import java.util.*;

public class Tabulator {

    public class KeyCount implements Comparable<KeyCount> {
        public final String key;
        public final int count;

        public KeyCount(String key, int count) {
            this.key = key;
            this.count = count;
        }

        @Override
        public int compareTo(KeyCount other) {
            assert other != null;
            return this.count - other.count;
        }
    }

    private Map<String, Integer> counts = new HashMap<String, Integer>();

    public void addKey(String key) {
        if (counts.containsKey(key)) {
            counts.put(key, counts.get(key) + 1);
        } else {
            counts.put(key, 1);
        }
    }

    public List<KeyCount> getSortedCountsAsc() {
        List<KeyCount> sortedCounts = new ArrayList<KeyCount>();
        for (String key : counts.keySet()) {
            int val = counts.get(key);
            sortedCounts.add(new KeyCount(key, val));
        }
        Collections.sort(sortedCounts);
        return sortedCounts;
    }

    public List<KeyCount> getSortedCountsDesc() {
        List<KeyCount> sorted = getSortedCountsAsc();
        Collections.reverse(sorted);
        return sorted;
    }

    public Map<String, Integer> getCounts() {
        return counts;
    }
}
