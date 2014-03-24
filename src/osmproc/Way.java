package osmproc;

import java.util.ArrayList;
import java.util.List;

public class Way {
    private List<String> nodeIds = new ArrayList<String>();

    public List<String> getNodeIds() {
        return nodeIds;
    }

    public void addNodeId(String nodeId) {
        this.nodeIds.add(nodeId);
    }

    public List<Tuple<String, String>> getNodeIdPairs() {
        List<Tuple<String, String>> nodeIdPairs = new ArrayList<Tuple<String, String>>();
        for (int i = 0; i < nodeIds.size() - 1; i++) {
            Tuple<String, String> nodeIdPair = new Tuple<String, String>(nodeIds.get(i), nodeIds.get(i+1));
            nodeIdPairs.add(nodeIdPair);
        }
        return nodeIdPairs;
    }

    public int size() {
        return nodeIds.size();
    }

    public boolean isCircular() {
        //noinspection SimplifiableIfStatement
        if (this.nodeIds.size() < 2) {
            return false;
        }
        return nodeIds.get(0).equals(nodeIds.get(size() - 1));
    }

    @Override
    public String toString() {
        return String.format("Way: %s nodes%s", size(), isCircular() ? " (circular)" : "");
    }
}
