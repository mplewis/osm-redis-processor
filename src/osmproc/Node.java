package osmproc;

public class Node {
    private String id;
    private float lat;
    private float lon;

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public float getLat() {
        return lat;
    }

    public void setLat(float lat) {
        this.lat = lat;
    }

    public float getLon() {
        return lon;
    }

    public void setLon(float lon) {
        this.lon = lon;
    }

    @Override
    public String toString() {
        return "Node (ID: " + getId() + ", Lat: " + getLat() + ", Lon: " + getLon() + ")";
    }
}
