package osmproc.io;

import osmproc.structure.Node;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.text.DecimalFormat;

public class NodePartitionBuffer {

    private String directory;
    private String filenameTemplate;
    private final DecimalFormat partitionPrecision;

    private Connection conn;
    private PreparedStatement insertNode;
    private PreparedStatement insertNodeAdj;

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
                "INSERT INTO nodes (node_id, latitude, longitude) VALUES (?, ?, ?);");
        this.insertNodeAdj = conn.prepareStatement(
                "INSERT INTO node_adjs (node_a, node_b) VALUES (?, ?);");
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

}
