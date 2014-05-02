package osmproc.io;

import osmproc.structure.Node;
import osmproc.structure.Tuple;

import java.sql.*;
import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.List;

public class NodePartitionBuffer {

    private String directory;
    private String filenameTemplate;
    private DecimalFormat partitionPrecision;

    private Connection conn;
    private PreparedStatement insertNode;
    private PreparedStatement insertNodeAdj;
    private PreparedStatement getPartitions;

    public NodePartitionBuffer(String directory, String filenameTemplate, DecimalFormat partitionPrecision)
            throws SQLException, ClassNotFoundException {

        // load the sqlite-JDBC driver using the current class loader
        Class.forName("org.sqlite.JDBC");

        this.directory = directory;
        this.filenameTemplate = filenameTemplate;
        this.partitionPrecision = partitionPrecision;
        this.conn = DriverManager.getConnection("jdbc:sqlite::memory:");
        conn.prepareStatement("CREATE TABLE nodes(id INTEGER PRIMARY KEY, node_id TEXT, latitude TEXT, " +
                "longitude TEXT, lat_part TEXT, lon_part TEXT);").execute();
        conn.prepareStatement("CREATE TABLE node_adjs(id INTEGER PRIMARY KEY, node_a TEXT, node_b TEXT);").execute();
        this.insertNode = conn.prepareStatement(
                "INSERT INTO nodes (node_id, latitude, longitude, lat_part, lon_part) VALUES (?, ?, ?, ?, ?);");
        this.insertNodeAdj = conn.prepareStatement(
                "INSERT INTO node_adjs (node_a, node_b) VALUES (?, ?);");
        this.getPartitions = conn.prepareStatement(
                "SELECT DISTINCT lat_part, lon_part FROM nodes ORDER BY lat_part, lon_part;");
    }

    public void commitNode(Node node) throws SQLException {
        String latPart = partitionPrecision.format(node.getLat());
        String lonPart = partitionPrecision.format(node.getLon());
        insertNode.setString(1, node.getNodeId());
        insertNode.setString(2, String.valueOf(node.getLat()));
        insertNode.setString(3, String.valueOf(node.getLon()));
        insertNode.setString(4, latPart);
        insertNode.setString(5, lonPart);
        insertNode.execute();
    }

    public void commitNodeAdjIds(String nodeAId, String nodeBId) throws SQLException {
        insertNodeAdj.setString(1, nodeAId);
        insertNodeAdj.setString(2, nodeBId);
        insertNodeAdj.execute();
    }

    public List<Tuple<String, String>> getPartitions() throws SQLException {
        List<Tuple<String, String>> partitions = new ArrayList<Tuple<String, String>>();
        ResultSet results = getPartitions.executeQuery();
        while (results.next()) {
            String latPart = results.getString(1);
            String lonPart = results.getString(2);
            partitions.add(new Tuple<String, String>(latPart, lonPart));
        }
        return partitions;
    }

}
