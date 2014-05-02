package osmproc.structure;

public class Node {
    private String nodeId;
    private double lat;
    private double lon;

    public Node() {}

    public Node(String nodeId, double lat, double lon) {
        this.nodeId = nodeId;
        this.lat = lat;
        this.lon = lon;
    }

    public String getNodeId() {
        return nodeId;
    }

    public void setNodeId(String nodeId) {
        this.nodeId = nodeId;
    }

    public double getLat() {
        return lat;
    }

    public void setLat(double lat) {
        this.lat = lat;
    }

    public double getLon() {
        return lon;
    }

    public void setLon(double lon) {
        this.lon = lon;
    }

    @Override
    public String toString() {
        return "Node{" +
                "nodeId='" + nodeId + '\'' +
                ", lat=" + lat +
                ", lon=" + lon +
                '}';
    }

}
