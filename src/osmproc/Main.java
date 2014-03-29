package osmproc;

import com.google.common.hash.BloomFilter;
import com.google.common.hash.Funnels;
import redis.clients.jedis.Jedis;

import javax.xml.stream.XMLEventReader;
import javax.xml.stream.XMLInputFactory;
import javax.xml.stream.events.Attribute;
import javax.xml.stream.events.EndElement;
import javax.xml.stream.events.StartElement;
import javax.xml.stream.events.XMLEvent;
import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

@SuppressWarnings({"PointlessBooleanExpression", "ConstantConditions"})
public class Main {

    /* Node processing settings */

    public static final boolean ADD_NODES = true;
    public static final boolean ADD_NODE_ADJACENCIES = true;

    public static final boolean FILTER_NODES_BY_AREA = true;
    public static final double LAT_MIN =  44.959454;
    public static final double LAT_MAX =  44.992362;
    public static final double LON_MIN = -93.250237;
    public static final double LON_MAX = -93.204060;
    public static final int EXPECTED_NODES_IN_AREA = 60000;
    public static final Area NODE_AREA = new Area(LAT_MIN, LAT_MAX, LON_MIN, LON_MAX);

    public static final boolean FILTER_NODES_BY_TAG = true;
    public static final List<String> ACCEPTABLE_TAG_KEYS = Arrays.asList("highway");

    public static final boolean COMMIT_DATA_TO_REDIS = false;

    public static final boolean TABULATE_TAGS = false;

    public static final String OSM_DATA_XML_PATH = "data/mpls-stpaul.osm";
    public static final String JEDIS_HOST = "localhost";

    /* XML properties: do not modify! */

    public static final String NODE_TAG = "node";
    public static final String WAY_TAG = "way";
    public static final String NODE_REF_TAG = "nd";

    public static final String NODE_ATTR_ID = "id";
    public static final String NODE_ATTR_LAT = "lat";
    public static final String NODE_ATTR_LON = "lon";

    public static final String NODE_REF_ATTR_ID = "ref";

    /* The bread and butter */

    public static void main(String[] args) {
        Tabulator<String> tagTab = new Tabulator<String>();

        try {
            Jedis jedis = new Jedis(JEDIS_HOST);
            File file = new File(OSM_DATA_XML_PATH);

            long startTime = System.currentTimeMillis();

            XMLInputFactory inputFactory = XMLInputFactory.newInstance();
            InputStream in = new FileInputStream(file);
            XMLEventReader eventReader = inputFactory.createXMLEventReader(in);

            Node node = new Node();
            Way way = new Way();
            String tagKey = null;

            BloomFilter<CharSequence> acceptedNodeIds = BloomFilter.create(
                    Funnels.stringFunnel(StandardCharsets.UTF_8), EXPECTED_NODES_IN_AREA, 0.01);

            int nodeCount = 0;
            int nodeAddedCount = 0;
            int wayCount = 0;
            int wayAddedCount = 0;
            int wayRejectedTagCount = 0;
            int wayRejectedAreaCount = 0;
            int wayRejectedTagAndAreaCount = 0;

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
                                node.setId(attribute.getValue());
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

                                if (TABULATE_TAGS) {
                                    tagTab.addKey(tagKey);
                                }
                            }
                        }

                    }

                } else if (event.isEndElement()) {

                    EndElement endElement = event.asEndElement();
                    if (ADD_NODES && endElement.getName().getLocalPart().equals(NODE_TAG)) {

                        if (!FILTER_NODES_BY_AREA || (FILTER_NODES_BY_AREA && NODE_AREA.contains(node))) {
                            commitNodeToRedis(node, jedis);
                            acceptedNodeIds.put(node.getId());
                            nodeAddedCount++;
                        }

                        nodeCount++;
                        if (nodeCount % 10000 == 0) {
                            float elapsed = (float) (System.currentTimeMillis() - startTime) / 1000;
                            System.out.println(String.format("%.3f: %s nodes processed, %s accepted",
                                    elapsed, nodeCount, nodeAddedCount));
                        }

                    } else if (ADD_NODE_ADJACENCIES && endElement.getName().getLocalPart().equals(WAY_TAG)) {

                        boolean rejectedTag = true;
                        boolean rejectedArea = true;

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

                        if (!FILTER_NODES_BY_AREA || (FILTER_NODES_BY_AREA && way.mightHaveNodesIn(acceptedNodeIds))) {
                            rejectedArea = false;
                        }

                        if (!rejectedTag && !rejectedArea) {
                            commitWayToRedis(way, jedis);
                            wayAddedCount++;
                        } else if (rejectedTag && rejectedArea) {
                            wayRejectedTagAndAreaCount++;
                        } else if (rejectedTag) {
                            wayRejectedTagCount++;
                        } else if (rejectedArea) {
                            wayRejectedAreaCount++;
                        }

                        wayCount++;
                        if (wayCount % 1000 == 0) {
                            float elapsed = (float) (System.currentTimeMillis() - startTime) / 1000;
                            System.out.println(String.format(
                                    "%.3f: %s ways processed, %s accepted, %s rejected by tag, " +
                                    "%s rejected by area, %s rejected by both",
                                    elapsed, wayCount, wayAddedCount, wayRejectedTagCount, wayRejectedAreaCount,
                                    wayRejectedTagAndAreaCount));
                        }

                    }
                }

            }
        } catch (Exception e) {
            e.printStackTrace();
        }

        if (TABULATE_TAGS) {
            List<TabCount<String>> tagCounts = tagTab.getSortedCountsDesc();
            for (TabCount<String> tagCount : tagCounts) {
                System.out.println(tagCount.key + ": " + tagCount.count);
            }
        }
    }

    /* Redis database functions */

    static void commitNodeToRedis(Node node, Jedis jedis) {
        if (COMMIT_DATA_TO_REDIS) {
            String key = String.format("node:%s", node.getId());
            String val = String.format("%s:%s", node.getLat(), node.getLon());
            jedis.set(key, val);
        }
    }

    static void commitWayToRedis(Way way, Jedis jedis) {
        if (COMMIT_DATA_TO_REDIS) {
            String first;
            String second;
            for (Tuple<String, String> nodeIdPair : way.getNodeIdPairs()) {
                first = nodeIdPair.x;
                second = nodeIdPair.y;
                commitNodeIdAdjToRedis(first, second, jedis);
                commitNodeIdAdjToRedis(second, first, jedis);
            }
        }
    }

    static void commitNodeIdAdjToRedis(String baseNodeId, String adjNodeId, Jedis jedis) {
        if (COMMIT_DATA_TO_REDIS) {
            String key = String.format("nodeadj:%s", baseNodeId);
            jedis.sadd(key, adjNodeId);
        }
    }

}
