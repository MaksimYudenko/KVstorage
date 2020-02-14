package by.webapp.kvstorage.util;

import by.webapp.kvstorage.model.Node;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;

import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class NodeLoader {

    private static final Logger logger = LogManager.getLogger(NodeLoader.class);
    public static Map<Integer, List<Node>> groupToNodesMap = new HashMap<>();
    private static Map<String, Node> nodeMap = new HashMap<>();
    public static Node thisNode;

    static {
        try {
            JSONObject nodesGroup = (JSONObject) new JSONParser()
                    .parse(new InputStreamReader(NodeLoader.class
                            .getResourceAsStream("/nodesGroup.json")));
            JSONArray groups = (JSONArray) nodesGroup.get("groups");
            for (Object groupItem : groups) {
                JSONObject item = (JSONObject) groupItem;
                Integer group = Integer.valueOf((String) item.get("id"));
                JSONArray nodesList = (JSONArray) item.get("list");
                List<Node> nodes = new ArrayList<>();
                for (Object nodeObject : nodesList) {
                    JSONObject nodeItem = (JSONObject) nodeObject;
                    String name = (String) nodeItem.get("name");
                    String url = (String) nodeItem.get("url");
                    Node node = new Node(name, url, group);
                    nodes.add(node);
                    nodeMap.put(name, node);
                }
                groupToNodesMap.put(group, nodes);
            }
        } catch (Exception e) {
            e.printStackTrace();
            final String message = "Error: NodeLoader failed.";
            logger.fatal(message, e);
            System.exit(1);
        }
    }

    public static void setNode(String nodeName) {
        if (nodeName == null) {
            nodeName = "node0";
        }
        thisNode = nodeMap.get(nodeName);
        System.setProperty("server.port", thisNode.getUrl().replace("http://localhost:", ""));
        try {
            JSONObject nodeProperties = (JSONObject) new JSONParser()
                    .parse(new InputStreamReader(NodeLoader.class
                            .getResourceAsStream("/nodeProps.json")));
            JSONObject property = (JSONObject) nodeProperties.get(nodeName);
            System.setProperty("spring.datasource.url", (String) property.get("url"));
            System.setProperty("spring.datasource.username", (String) property.get("username"));
            System.setProperty("spring.datasource.password", (String) property.get("password"));
        } catch (Exception e) {
            e.printStackTrace();
            final String message = "Error: NodeLoader failed in setNode().";
            logger.fatal(message, e);
            System.exit(1);
        }

    }

}