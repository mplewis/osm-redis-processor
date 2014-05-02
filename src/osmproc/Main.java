package osmproc;

import com.google.gson.Gson;
import com.google.gson.stream.JsonWriter;
import osmproc.io.NodePartitionBuffer;
import osmproc.structure.Node;
import osmproc.structure.Tuple;
import osmproc.structure.Way;

import javax.xml.stream.XMLEventReader;
import javax.xml.stream.XMLInputFactory;
import javax.xml.stream.events.Attribute;
import javax.xml.stream.events.EndElement;
import javax.xml.stream.events.StartElement;
import javax.xml.stream.events.XMLEvent;
import java.io.*;
import java.math.RoundingMode;
import java.text.DecimalFormat;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

@SuppressWarnings({"PointlessBooleanExpression", "ConstantConditions"})
public class Main {

    /* Node processing settings */

    public static final boolean ADD_NODES = true;
    public static final boolean ADD_NODE_ADJACENCIES = true;

    public static final DecimalFormat PARTITION_PRECISION = new DecimalFormat("0.000"); // 3 zeroes

    public static final boolean FILTER_NODES_BY_TAG = true;
    public static final List<String> ACCEPTABLE_TAG_KEYS = Arrays.asList("highway");

    public static final String PARTITION_FILE_DIRECTORY = "output";
    public static final String PARTITION_FILE_NAME_TEMPLATE = "%s_%s.json"; // lat, lng
    public static final String PARTITION_MAP_FILE_NAME = "node_partition_map.json"; // lat, lng

    /* XML properties: do not modify! */

    public static final String NODE_TAG = "node";
    public static final String WAY_TAG = "way";
    public static final String NODE_REF_TAG = "nd";

    public static final String NODE_ATTR_ID = "id";
    public static final String NODE_ATTR_LAT = "lat";
    public static final String NODE_ATTR_LON = "lon";

    public static final String NODE_REF_ATTR_ID = "ref";

    /* The bread and butter */

    public static void main(String[] args) throws Exception {
        // Ensure all coord partition pairs are rounded down
        PARTITION_PRECISION.setRoundingMode(RoundingMode.DOWN);

        NodePartitionBuffer buf = new NodePartitionBuffer(
                PARTITION_FILE_DIRECTORY, PARTITION_FILE_NAME_TEMPLATE, PARTITION_PRECISION);

        final String OSM_DATA_XML_PATH = args[0];

        long startTime = System.currentTimeMillis();

        try {
            File file = new File(OSM_DATA_XML_PATH);

            XMLInputFactory inputFactory = XMLInputFactory.newInstance();
            InputStream in = new FileInputStream(file);
            XMLEventReader eventReader = inputFactory.createXMLEventReader(in);

            Node node = null;
            Way way = new Way();
            String tagKey = null;

            int nodeCount = 0;
            int wayCount = 0;
            int wayAddedCount = 0;
            int wayRejectedTagCount = 0;

            while (eventReader.hasNext()) {
                XMLEvent event = eventReader.nextEvent();

                if (event.isStartElement()) {

                    StartElement startElement = event.asStartElement();
                    if (ADD_NODES && startElement.getName().getLocalPart().equals(NODE_TAG)) {

                        node = new Node();
                        Iterator attributes = startElement.getAttributes();

                        while (attributes.hasNext()) {
                            Attribute attribute = (Attribute) attributes.next();

                            if (attribute.getName().toString().equals(NODE_ATTR_ID)) {
                                node.setNodeId(attribute.getValue());
                            } else if (attribute.getName().toString().equals(NODE_ATTR_LAT)) {
                                node.setLat(Float.parseFloat(attribute.getValue()));
                            } else if (attribute.getName().toString().equals(NODE_ATTR_LON)) {
                                node.setLon(Float.parseFloat(attribute.getValue()));
                            }

                        }

                    } else if (ADD_NODE_ADJACENCIES && startElement.getName().getLocalPart().equals(WAY_TAG)) {

                        way = new Way();
                        tagKey = null;

                    } else if (ADD_NODE_ADJACENCIES && startElement.getName().getLocalPart().equals(NODE_REF_TAG)) {

                        Iterator attributes = startElement.getAttributes();
                        while (attributes.hasNext()) {
                            Attribute attribute = (Attribute) attributes.next();
                            if (attribute.getName().toString().equals(NODE_REF_ATTR_ID)) {
                                way.addNodeId(attribute.getValue());
                            }
                        }

                    } else if (ADD_NODE_ADJACENCIES && startElement.getName().getLocalPart().equals("tag")) {

                        Iterator attributes = startElement.getAttributes();
                        while (attributes.hasNext()) {
                            Attribute attribute = (Attribute) attributes.next();

                            if (attribute.getName().toString().equals("k")) {
                                tagKey = attribute.getValue();

                            } else if (attribute.getName().toString().equals("v")) {
                                String tagVal = attribute.getValue();
                                way.addTag(tagKey, tagVal);
                            }
                        }

                    }

                } else if (event.isEndElement()) {

                    EndElement endElement = event.asEndElement();
                    if (ADD_NODES && endElement.getName().getLocalPart().equals(NODE_TAG)) {
                        buf.commitNode(node);

                        nodeCount++;
                        if (nodeCount % 10000 == 0) {
                            float elapsed = (float) (System.currentTimeMillis() - startTime) / 1000;
                            System.out.println(String.format("%.3f: %s nodes processed and accepted",
                                    elapsed, nodeCount));
                        }

                    } else if (ADD_NODE_ADJACENCIES && endElement.getName().getLocalPart().equals(WAY_TAG)) {

                        boolean rejectedTag = true;

                        if (FILTER_NODES_BY_TAG) { // Filtering nodes by ACCEPTABLE_TAG_KEYS
                            for (String key : ACCEPTABLE_TAG_KEYS) {
                                if (way.getTags().containsKey(key)) {
                                    rejectedTag = false;
                                    break; // Short-circuit: once an acceptable tag is found, the way can be added
                                }
                            }
                        } else {
                            rejectedTag = false;
                        }

                        if (!rejectedTag) {
                            for (Tuple<String, String> nodeIdPair : way.getNodeIdPairs()) {
                                String first = nodeIdPair.x;
                                String second = nodeIdPair.y;
                                buf.commitNodeAdjIds(first, second);
                                buf.commitNodeAdjIds(second, first);
                            }

                            wayAddedCount++;
                        } else if (rejectedTag) {
                            wayRejectedTagCount++;
                        }

                        wayCount++;
                        if (wayCount % 1000 == 0) {
                            float elapsed = (float) (System.currentTimeMillis() - startTime) / 1000;
                            System.out.println(String.format(
                                    "%.3f: %s ways processed, %s accepted, %s rejected by tag",
                                    elapsed, wayCount, wayAddedCount, wayRejectedTagCount));
                        }

                    }
                }

            }
        } catch (Exception e) {
            e.printStackTrace();
        }

        Gson gson = new Gson();
        String partMapPath = new File(PARTITION_FILE_DIRECTORY, PARTITION_MAP_FILE_NAME).toString();
        JsonWriter partMapWriter = new JsonWriter(new BufferedWriter(new FileWriter(partMapPath)));
        int nodePartCount = 0;

        List<Tuple<String, String>> partitions = buf.getPartitions();
        partMapWriter.beginObject();
        for (Tuple<String, String> p : partitions) {
            List<Node> nodes = buf.getNodesForPartition(p.x, p.y);
            String partFileName = buf.partcodeFromTemplate(p.x, p.y);
            String partPath = new File(PARTITION_FILE_DIRECTORY, partFileName).toString();
            JsonWriter partWriter = new JsonWriter(new BufferedWriter(new FileWriter(partPath)));
            partWriter.beginObject();
            for (Node node : nodes) {
                String nodeId = node.getNodeId();
                String partitionCode = buf.partcodeFromTemplate(p.x, p.y);
                partMapWriter.name(nodeId).value(partitionCode);

                partWriter.name(nodeId);

                partWriter.beginObject();
                partWriter.name("lat").value(node.getLat());
                partWriter.name("lon").value(node.getLon());
                partWriter.name("adj");

                partWriter.beginArray();
                for (String adj : buf.getNodeAdjsForNodeId(nodeId)) {
                    partWriter.value(adj);
                }
                partWriter.endArray();

                partWriter.endObject();

                nodePartCount++;
                if (nodePartCount % 10 == 0) {
                    float elapsed = (float) (System.currentTimeMillis() - startTime) / 1000;
                    System.out.println(String.format(
                            "%.3f: %s node parts processed", elapsed, nodePartCount));
                }
            }
            partWriter.endObject();
            partWriter.close();
        }
        partMapWriter.endObject();
        partMapWriter.close();
    }

}
