package osmproc;

import com.google.gson.stream.JsonWriter;
import osmproc.structure.Area;
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
import java.util.*;

@SuppressWarnings({"PointlessBooleanExpression", "ConstantConditions"})
public class Main {

    /* Node processing settings */

    public static final boolean FILTER_NODES_BY_TAG = true;
    public static final List<String> ACCEPTABLE_TAG_KEYS = Arrays.asList("highway");

    public static final String INPUT_OSM_XML_PATH = "data/mpls-stpaul.osm";
    public static final String OUTPUT_JSON_PATH = "output/nodes.json";

    public static final double LAT_MIN =  44.959454;
    public static final double LAT_MAX =  44.992362;
    public static final double LON_MIN = -93.250237;
    public static final double LON_MAX = -93.204060;
    public static final Area NODE_AREA = new Area(LAT_MIN, LAT_MAX, LON_MIN, LON_MAX);

    /* XML properties: do not modify! */

    public static final String NODE_TAG = "node";
    public static final String WAY_TAG = "way";
    public static final String NODE_REF_TAG = "nd";

    public static final String NODE_ATTR_ID = "id";
    public static final String NODE_ATTR_LAT = "lat";
    public static final String NODE_ATTR_LON = "lon";

    public static final String NODE_REF_ATTR_ID = "ref";

    /* The bread and butter */

    public static Set<Node> nodesInArea = new HashSet<Node>();
    public static Set<String> nodeIdsInArea = new HashSet<String>();
    public static Map<String, Set<String>> nodeAdjs = new HashMap<String, Set<String>>();
    public static Set<String> taggedNodeIds = new HashSet<String>();

    public static void main(String[] args) throws Exception {
        long startTime = System.currentTimeMillis();

        int nodeCount = 0;
        int nodeAcceptedCount = 0;
        int nodeRejectedAreaCount = 0;
        int wayCount = 0;
        int wayAddedCount = 0;
        int wayRejectedTagCount = 0;

        try {
            File file = new File(INPUT_OSM_XML_PATH);

            XMLInputFactory inputFactory = XMLInputFactory.newInstance();
            InputStream in = new FileInputStream(file);
            XMLEventReader eventReader = inputFactory.createXMLEventReader(in);

            Node node = null;
            Way way = new Way();
            String tagKey = null;

            while (eventReader.hasNext()) {
                XMLEvent event = eventReader.nextEvent();

                if (event.isStartElement()) {

                    StartElement startElement = event.asStartElement();
                    if (startElement.getName().getLocalPart().equals(NODE_TAG)) {

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

                    } else if (startElement.getName().getLocalPart().equals(WAY_TAG)) {

                        way = new Way();
                        tagKey = null;

                    } else if (startElement.getName().getLocalPart().equals(NODE_REF_TAG)) {

                        Iterator attributes = startElement.getAttributes();
                        while (attributes.hasNext()) {
                            Attribute attribute = (Attribute) attributes.next();
                            if (attribute.getName().toString().equals(NODE_REF_ATTR_ID)) {
                                way.addNodeId(attribute.getValue());
                            }
                        }

                    } else if (startElement.getName().getLocalPart().equals("tag")) {

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
                    if (endElement.getName().getLocalPart().equals(NODE_TAG)) {

                        if (NODE_AREA.contains(node)) {
                            nodesInArea.add(node);
                            nodeIdsInArea.add(node.getNodeId());
                            nodeAcceptedCount++;
                        } else {
                            nodeRejectedAreaCount++;
                        }

                        nodeCount++;
                        if (nodeCount % 10000 == 0) {
                            float elapsed = (float) (System.currentTimeMillis() - startTime) / 1000;
                            System.out.println(String.format(
                                    "%.3f: %s nodes processed, %s accepted, %s rejected by area",
                                    elapsed, nodeCount, nodeAcceptedCount, nodeRejectedAreaCount));
                        }

                    } else if (endElement.getName().getLocalPart().equals(WAY_TAG)) {

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
                            taggedNodeIds.addAll(way.getNodeIds());

                            for (Tuple<String, String> nodeIdPair : way.getNodeIdPairs()) {
                                String first = nodeIdPair.x;
                                String second = nodeIdPair.y;

                                // Don't add adjacencies for nodes not in area
                                if (!nodeIdsInArea.contains(first) || !nodeIdsInArea.contains(second)) {
                                    continue;
                                }

                                Set<String> firstSet = nodeAdjs.get(first);
                                Set<String> secondSet = nodeAdjs.get(second);
                                if (firstSet == null) {
                                    firstSet = new HashSet<String>();
                                    nodeAdjs.put(first, firstSet);
                                }
                                if (secondSet == null) {
                                    secondSet = new HashSet<String>();
                                    nodeAdjs.put(second, secondSet);
                                }
                                firstSet.add(second);
                                secondSet.add(first);
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

        System.out.println(String.format("%.3f: %s nodes processed total, %s ways processed total",
                (float) (System.currentTimeMillis() - startTime) / 1000, nodeCount, wayCount));

        int nodesCheckedForTag = 0;
        int nodesTrimmedByTag = 0;
        int nodesNotTrimmedByTag = 0;
        List<Node> trimmedNodesInArea = new ArrayList<Node>();

        for (Node node : nodesInArea) {
            if (taggedNodeIds.contains(node.getNodeId())) {
                trimmedNodesInArea.add(node);
                nodesNotTrimmedByTag++;
            } else {
                nodesTrimmedByTag++;
            }
            nodesCheckedForTag++;
            if (nodesCheckedForTag % 1000 == 0) {
                float elapsed = (float) (System.currentTimeMillis() - startTime) / 1000;
                System.out.println(String.format(
                        "%.3f: %s nodes checked for tag, %s accepted, %s trimmed by tag",
                        elapsed, nodesCheckedForTag, nodesNotTrimmedByTag, nodesTrimmedByTag));
            }
        }

        System.out.println(String.format("%.3f: %s nodes accepted total",
                (float) (System.currentTimeMillis() - startTime) / 1000, nodesNotTrimmedByTag));

        JsonWriter partMapWriter = new JsonWriter(new BufferedWriter(new FileWriter(new File(OUTPUT_JSON_PATH))));
        int nodeWrittenCount = 0;

        partMapWriter.beginObject();

        for (Node node : trimmedNodesInArea) {
            partMapWriter.name(node.getNodeId());
            partMapWriter.beginObject();
            partMapWriter.name("lat").value(node.getLat());
            partMapWriter.name("lon").value(node.getLon());
            partMapWriter.name("adj");
            partMapWriter.beginArray();
            Set<String> singleNodeAdjs = nodeAdjs.get(node.getNodeId());
            if (singleNodeAdjs != null) {
                for (String adj : singleNodeAdjs) {
                    partMapWriter.value(adj);
                }
            }
            partMapWriter.endArray();
            partMapWriter.endObject();

            nodeWrittenCount++;
            if (nodeWrittenCount % 1000 == 0) {
                float elapsed = (float) (System.currentTimeMillis() - startTime) / 1000;
                System.out.println(String.format("%.3f: %s nodes written", elapsed, nodeWrittenCount));
            }
        }

        partMapWriter.endObject();
        partMapWriter.close();

        System.out.println(String.format("%.3f: %s nodes written total",
                (float) (System.currentTimeMillis() - startTime) / 1000, nodeWrittenCount));
    }

}
