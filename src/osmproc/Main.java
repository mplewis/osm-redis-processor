package osmproc;

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
import java.util.Iterator;
import java.util.List;

@SuppressWarnings({"PointlessBooleanExpression", "ConstantConditions"})
public class Main {

    /* Node processing settings */

    public static final boolean ADD_NODES = false;
    public static final boolean ADD_NODE_ADJ = true;

    public static final boolean FILTER_NODES_BY_AREA = true;
    public static final double LAT_MIN =  44.959454;
    public static final double LAT_MAX =  44.992362;
    public static final double LON_MIN = -93.250237;
    public static final double LON_MAX = -93.204060;
    public static final Area NODE_AREA = new Area(LAT_MIN, LAT_MAX, LON_MIN, LON_MAX);

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
        Tabulator tagTab = new Tabulator();

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

            int nodeCount = 0;
            int nodeAddedCount = 0;
            int wayCount = 0;

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

                    } else if (ADD_NODE_ADJ && startElement.getName().getLocalPart().equals(WAY_TAG)) {

                        way = new Way();
                        tagKey = null;

                    } else if (ADD_NODE_ADJ && startElement.getName().getLocalPart().equals(NODE_REF_TAG)) {

                        Iterator attributes = startElement.getAttributes();
                        while (attributes.hasNext()) {
                            Attribute attribute = (Attribute) attributes.next();
                            if (attribute.getName().toString().equals(NODE_REF_ATTR_ID)) {
                                way.addNodeId(attribute.getValue());
                            }
                        }

                    } else if (ADD_NODE_ADJ && startElement.getName().getLocalPart().equals("tag")) {

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
                            nodeAddedCount++;
                        }

                        nodeCount++;
                        if (nodeCount % 10000 == 0) {
                            float elapsed = (float) (System.currentTimeMillis() - startTime) / 1000;
                            System.out.println(String.format("%.3f: %s nodes processed, %s accepted",
                                    elapsed, nodeCount, nodeAddedCount));
                        }

                    } else if (ADD_NODE_ADJ && endElement.getName().getLocalPart().equals(WAY_TAG)) {

                        commitWayToRedis(way, jedis);

                        wayCount++;
                        if (wayCount % 1000 == 0) {
                            float elapsed = (float) (System.currentTimeMillis() - startTime) / 1000;
                            System.out.println(String.format("%.3f: %s ways processed", elapsed, wayCount));
                        }

                    }
                }

            }
        } catch (Exception e) {
            e.printStackTrace();
        }

        if (TABULATE_TAGS) {
            List<Tabulator.KeyCount> tagCounts = tagTab.getSortedCountsDesc();
            for (Tabulator.KeyCount tagCount : tagCounts) {
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
                commitNodeAdjToRedis(first, second, jedis);
                commitNodeAdjToRedis(second, first, jedis);
            }
        }
    }

    static void commitNodeAdjToRedis(String baseNode, String adjNode, Jedis jedis) {
        if (COMMIT_DATA_TO_REDIS) {
            String key = String.format("nodeadj:%s", baseNode);
            jedis.sadd(key, adjNode);
        }
    }

}
