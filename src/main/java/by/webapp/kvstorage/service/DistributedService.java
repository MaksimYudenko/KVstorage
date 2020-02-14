package by.webapp.kvstorage.service;

import by.webapp.kvstorage.exception.BadRequestException;
import by.webapp.kvstorage.exception.FailedException;
import by.webapp.kvstorage.exception.ResourceNotFoundException;
import by.webapp.kvstorage.model.Collection;
import by.webapp.kvstorage.model.Document;
import by.webapp.kvstorage.model.Node;
import by.webapp.kvstorage.util.NodeLoader;
import by.webapp.kvstorage.util.Validator;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.json.JSONObject;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.core.ParameterizedTypeReference;
import org.springframework.http.*;
import org.springframework.stereotype.Component;
import org.springframework.web.client.HttpClientErrorException;
import org.springframework.web.client.HttpServerErrorException;
import org.springframework.web.client.ResourceAccessException;
import org.springframework.web.client.RestTemplate;

import java.util.*;
import java.util.stream.Collectors;

@Component
public class DistributedService {

    private static final Logger logger = LogManager.getLogger(DistributedService.class);
    private final RestTemplate restTemplate;
    private Map<Integer, List<Node>> groupToNodesMap;
    private final List<Node> nodeList = new ArrayList<>();

    @Autowired
    public DistributedService(RestTemplate restTemplate) {
        this.restTemplate = restTemplate;
        groupToNodesMap = NodeLoader.groupToNodesMap;
        groupToNodesMap.values().forEach(nodeList::addAll);
    }

//------------------------------     Collection distributing     ------------------------------

    public void distributeCreating(
            Object object, int counter, boolean shouldRollBack) {
        Node node = getReceivingNode(counter, shouldRollBack, nodeList);
        counter = shouldRollBack ? counter - 1 : counter + 1;
        if (node == null) {
            return;
        }
        try {
            restTemplate.postForEntity(assembleURL(node.getUrl()),
                    getEntity(object, getHeaders(counter, shouldRollBack)), Object.class);
        } catch (HttpServerErrorException.ServiceUnavailable e) {
            final String message =
                    "Exception in DistributeService.distributeCreating() " +
                            "while executing postForEntity() in " + node.getName();
            logger.error(message);
            throw new FailedException(message, e);
        } catch (ResourceAccessException e) {
            final String message = "Exception in DistributeService.distributeCreating() "
                    + node.getName() + " unavailable.";
            logger.error(message);
            throw new FailedException(message, e);
        }
    }

    public void distributeUpdating(
            Object object, int counter, boolean shouldRollBack, String... args) {
        Node node = getReceivingNode(counter, shouldRollBack, nodeList);
        counter = shouldRollBack ? counter - 1 : counter + 1;
        if (node == null) {
            return;
        }
        try {
            restTemplate.put(assembleURL(node.getUrl(), args),
                    getEntity(object, getHeaders(counter, shouldRollBack)));
        } catch (HttpServerErrorException.ServiceUnavailable e) {
            final String message =
                    "Exception in DistributeService.distributeUpdating()" +
                            " while executing put() in " + node.getName();
            logger.error(message);
            throw new FailedException(message, e);
        } catch (ResourceAccessException e) {
            final String message = "Exception in DistributeService.distributeUpdating(): "
                    + node.getName() + "unavailable.";
            logger.error(message);
            throw new FailedException(message, e);
        }
    }

    public void distributeDeleting(
            int counter, boolean shouldRollBack, String... args) {
        Node node = getReceivingNode(counter, shouldRollBack, nodeList);
        counter = shouldRollBack ? counter - 1 : counter + 1;
        if (node == null) {
            return;
        }
        try {
            restTemplate.exchange(assembleURL(node.getUrl(), args),
                    HttpMethod.DELETE, new HttpEntity<>(
                            getHeaders(counter, shouldRollBack)), Object.class);
        } catch (HttpServerErrorException.ServiceUnavailable e) {
            final String message = "Exception in DistributeService.distributeDeleting() " +
                    "while executing exchange() in " + node.getName();
            logger.error(message);
            throw new FailedException(message, e);
        } catch (ResourceAccessException e) {
            final String message = "Exception in DistributeService.distributeUpdating(): "
                    + node.getName() + "unavailable.";
            logger.error(message);
            throw new FailedException(message, e);
        }
    }

    public List<by.webapp.kvstorage.model.Collection> distributeGettingList(int offSet, int pageSize) {
        List<Node> list = nodeList;
        list.remove(NodeLoader.thisNode);
        boolean groupIsNotAvailable = true;
        List<Object> objects = null;
        for (Node node : list) {
            try {
                objects = restTemplate.exchange(
                        assembleURL(node.getUrl()),
                        HttpMethod.GET,
                        new HttpEntity<>(getReplicaHeaders()),
                        new ParameterizedTypeReference<List<Object>>() {
                        },
                        getRequestParams(offSet, pageSize))
                        .getBody();
                if (objects != null) {
                    groupIsNotAvailable = false;
                    break;
                }
            } catch (HttpServerErrorException.ServiceUnavailable e) {
                logger.error("LIST request is failed in " + node.getName(), e);
            } catch (ResourceAccessException e) {
                logger.error("Node " + node.getName() + " is unavailable.", e);
            }
        }
        if (groupIsNotAvailable) {
            final String message = "Group  " + NodeLoader.thisNode.getGroup() + " is unavailable.";
            logger.error(message);
            throw new FailedException(message);
        }
        List<by.webapp.kvstorage.model.Collection> collectionList = objects.stream()
                .map((obj) -> (Collection) obj).collect(Collectors.toList());
        return Validator.getCollectionSubList(collectionList, offSet, pageSize);
    }

//------------------------------     Document distributing     ------------------------------

    public void sendPost(
            Object object, int counter, boolean shouldRollBack, String... args) {
        List<Node> nodes = groupToNodesMap.get(
                defineGroup(args[0] + "/" + args[1]));
        Node node = getReceivingNode(counter, shouldRollBack, nodes);
        if (node == null) {
            return;
        }
        counter = shouldRollBack ? counter - 1 : counter + 1;
        try {
            restTemplate.postForEntity(assembleURL(node.getUrl(), args[0]),
                    getEntity(object, getHeaders(counter, shouldRollBack)), Object.class);
        } catch (HttpServerErrorException.ServiceUnavailable e) {
            throw new FailedException("Exception in sendPost() ", e);
        } catch (ResourceAccessException e) {
            logger.error("Node " + node.getName() + " is unavailable.", e);
            throw new ResourceAccessException("Exception in sendPost() " + e);
        }
    }

    public Document redirectPost(Object object, String... args) {
        Node node = groupToNodesMap.get(
                defineGroup(args[0] + "/" + args[1])).get(0);
        Document result = null;
        try {
            LinkedHashMap body = (LinkedHashMap) restTemplate.postForEntity(
                    assembleURL(node.getUrl(), args[0]),
                    getEntity(object, getHeaders()), Object.class).getBody();
            assert body != null;
            result = new Document();
            result.setKey((String) body.get("key"));
            result.setValue((String) body.get("value"));
        } catch (ResourceAccessException e) {
            final String message = "Node " + node.getName() + " is unavailable";
            logger.error(message);
            throw new FailedException(message, e);
        } catch (HttpClientErrorException e) {
            logger.error("ClientError is received from " + node.getName());
            throwClientException(e);
        } catch (HttpServerErrorException.ServiceUnavailable e) {
            throw new FailedException("Exception in redirectPost() ", e);
        }
        return result;
    }

    public Object sendGet(Class classType, String... parameters) {
        List<Node> list = new ArrayList<>(groupToNodesMap.get(NodeLoader.thisNode.getGroup()));
        list.remove(NodeLoader.thisNode);
        for (Node node : list) {
            try {
                ResponseEntity responseEntity = restTemplate
                        .exchange(assembleURL(node.getUrl(), parameters), HttpMethod.GET,
                                new HttpEntity<>(getReplicaHeaders()), classType);
                return responseEntity.getBody();
            } catch (ResourceAccessException e) {
                logger.error("Node " + node.getName() + " is unavailable.", e);
            } catch (HttpServerErrorException.ServiceUnavailable e) {
                throw new FailedException("Exception in sendGet() ", e);
            } catch (HttpClientErrorException e) {
                throw new ResourceNotFoundException(e.getMessage());
            }
        }
        throw new FailedException("Exception in sendGet().");
    }

    public Object redirectGet(Class classType, String... args) {
        int idGroup = defineGroup(args[0] + "/" + args[1]);
        List<Node> list = groupToNodesMap.get(idGroup);
        for (Node node : list) {
            try {
                ResponseEntity response = restTemplate.exchange
                        (assembleURL(node.getUrl(), args[0] + "/" + args[1]),
                                HttpMethod.GET,
                                new HttpEntity<>(getHeaders()), classType);
                return response.getBody();
            } catch (ResourceAccessException e) {
                logger.error("Node " + node.getName() + " is unavailable.", e);
            } catch (HttpServerErrorException.ServiceUnavailable e) {
                throw new FailedException("Exception in redirectGet() ", e);
            } catch (HttpClientErrorException e) {
                logger.error("ClientError is received from " + node.getName(), e);
                JSONObject json = new JSONObject(e.getResponseBodyAsString());
                throw new ResourceNotFoundException(json.getString("message"));
            }
        }
        throw new FailedException("Exception in redirectGet().");
    }

    public void sendUpdate(
            Object object, int counter, boolean shouldRollBack, String... args) {
        List<Node> nodes = groupToNodesMap.get(NodeLoader.thisNode.getGroup());
        Node node = getReceivingNode(counter, shouldRollBack, nodes);
        if (node == null) {
            return;
        }
        counter = shouldRollBack ? counter - 1 : counter + 1;
        try {
            restTemplate.put(assembleURL(node.getUrl(), args),
                    getEntity(object, getHeaders(counter, shouldRollBack)));
        } catch (HttpServerErrorException.ServiceUnavailable e) {
            throw new FailedException("Exception in sendUpdate()", e);
        } catch (ResourceAccessException e) {
            logger.error("Node " + node.getName() + " is unavailable.");
            throw new ResourceAccessException(
                    "Node " + node.getName() + " is unavailable." + e);
        }
    }

    public void redirectUpdate(Object object, String... args) {
        Node node = groupToNodesMap.get(
                defineGroup(args[0] + "/" + args[1])).get(0);
        try {
            restTemplate.exchange(
                    assembleURL(node.getUrl(), args),
                    HttpMethod.PUT,
                    getEntity(object, getHeaders()), Object.class);
        } catch (ResourceAccessException e) {
            final String message = "Node " + node.getName() + " is unavailable.";
            logger.error(message, e);
            throw new FailedException(message, e);
        } catch (HttpClientErrorException e) {
            logger.error("ClientError is received from " + node.getName(), e);
            throwClientException(e);
        } catch (HttpServerErrorException.ServiceUnavailable e) {
            throw new FailedException("Exception in redirectUpdate() ", e);
        }
    }

    public void sendDelete(
            int counter, boolean shouldRollBack, String... args) {
        List<Node> nodes = groupToNodesMap.get(
                defineGroup(args[0] + "/" + args[1]));
        Node node = getReceivingNode(counter, shouldRollBack, nodes);
        if (node == null) {
            return;
        }
        counter = shouldRollBack ? counter - 1 : counter + 1;
        try {
            restTemplate.exchange(assembleURL(node.getUrl(), args),
                    HttpMethod.DELETE, new HttpEntity<>(
                            getHeaders(counter, shouldRollBack)), Object.class);
        } catch (HttpServerErrorException.ServiceUnavailable e) {
            throw new FailedException("Exception in sendDelete() ", e);
        } catch (ResourceAccessException e) {
            logger.error("Node " + node.getName() + " is unavailable.", e);
            throw new ResourceAccessException("Node " + node.getName() +
                    " is unavailable." + e);
        }
    }

    public void redirectDelete(String... args) {
        Node node = groupToNodesMap.get(
                defineGroup(args[0] + "/" + args[1])).get(0);
        try {
            restTemplate.exchange(assembleURL(node.getUrl(), args),
                    HttpMethod.DELETE, new HttpEntity<>(getHeaders()), Object.class);
        } catch (ResourceAccessException e) {
            final String message = "Node " + node.getName() + " is unavailable.";
            logger.error(message, e);
            throw new FailedException(message, e);
        } catch (HttpClientErrorException e) {
            logger.error("ClientError is received from " + node.getName(), e);
            throwClientException(e);
        } catch (HttpServerErrorException.ServiceUnavailable e) {
            throw new FailedException("Exception in redirectDelete() ", e);
        }
    }

    public List<Document> distributeDocumentList(
            int offSet, int pageSize, String collectionId, List<Document> documents) {
        Map<Integer, List<Node>> mapGroups = new HashMap<>(groupToNodesMap);
        mapGroups.remove(NodeLoader.thisNode.getGroup());
        for (Map.Entry<Integer, List<Node>> group : mapGroups.entrySet()) {
            boolean isAvailable = false;
            for (Node node : group.getValue()) {
                try {
                    List<Document> body = restTemplate.exchange(
                            assembleURL(node.getUrl(), collectionId),
                            HttpMethod.GET,
                            new HttpEntity<>(getNotMainGroupHeaders()),
                            new ParameterizedTypeReference<List<Document>>() {
                            },
                            getRequestParams(offSet, pageSize))
                            .getBody();
                    if (body != null) {
                        documents.addAll(body);
                        isAvailable = true;
                        break;
                    }
                } catch (ResourceAccessException e) {
                    logger.error("Node " + node.getName() + " is unavailable.", e);
                } catch (HttpServerErrorException.ServiceUnavailable e) {
                    break;
                }
            }
            if (!isAvailable) {
                final String message = "Group  " + group.getKey() + " is unavailable.";
                logger.error(message);
                throw new FailedException(message);
            }
        }
        return Validator.getDocumentSubList(documents, offSet, pageSize);
    }

    public List<Object> sendListToReplica(
            int offSet, int size, String collectionId) {
        List<Node> list = new ArrayList<>(groupToNodesMap.get(NodeLoader.thisNode.getGroup()));
        list.remove(NodeLoader.thisNode);
        boolean groupIsNotAvailable = true;
        List<Object> objects = null;
        for (Node node : list) {
            try {
                objects = restTemplate.exchange(
                        assembleURL(node.getUrl(), collectionId),
                        HttpMethod.GET,
                        new HttpEntity<>(getReplicaHeaders()),
                        new ParameterizedTypeReference<List<Object>>() {
                        },
                        getRequestParams(offSet, size))
                        .getBody();
                if (objects != null) {
                    groupIsNotAvailable = false;
                    break;
                }
            } catch (HttpServerErrorException.ServiceUnavailable e) {
                logger.error("LIST request is failed in " + node.getName(), e);
            } catch (ResourceAccessException e) {
                logger.error("Node " + node.getName() + " is unavailable.", e);
            }
        }
        if (groupIsNotAvailable) {
            final String message = "Group  " + NodeLoader.thisNode.getGroup() + " is unavailable.";
            logger.error(message);
            throw new FailedException(message);
        }
        return objects;
    }

//-----------------------------------     Util methods     -----------------------------------

    public boolean isMyGroup(String id) {
        return defineGroup(id) == NodeLoader.thisNode.getGroup();
    }

    private int defineGroup(String id) {
        return Math.abs(id.hashCode()) % groupToNodesMap.size();
    }

    private Node getReceivingNode(
            int counter, boolean shouldRollBack, List<Node> list) {
        if (shouldRollBack) {
            return counter == 0 ? null : list.get(definePreviousIndex(list));
        } else {
            return counter == list.size() - 1 ? null : list.get(getNextIndex(list));
        }
    }

    private int getNextIndex(List<Node> list) {
        int index = 0;
        for (int i = 0; i < list.size(); i++) {
            if (list.get(i) == NodeLoader.thisNode) {
                if (i == list.size() - 1) {
                    return 0;
                } else {
                    index = i + 1;
                    break;
                }
            }
        }
        return index;
    }

    private int definePreviousIndex(List<Node> list) {
        int index = 0;
        for (int i = 0; i < list.size(); i++) {
            if (list.get(i) == NodeLoader.thisNode) {
                index = i - 1;
                if ((i - 1) < 0) {
                    index = list.size() - 1;
                }
                break;
            }
        }
        return index;
    }

    private String assembleURL(String host, String... args) {
        final String baseUrl = host + "/collections/";
        if (args == null || args.length == 0) {
            return baseUrl;
        } else {
            if (args.length == 1) {
                return baseUrl + args[0];
            }
            if (args.length == 2) {
                return baseUrl + args[0] + '/' + args[1];
            }
        }
        return null;
    }

    private HttpEntity getEntity(Object object, HttpHeaders headers) {
        return new HttpEntity<>(object, headers);
    }

    private Map<String, Integer> getRequestParams(
            Integer page, Integer pageSize) {
        return new HashMap<String, Integer>() {{
            put("page", page);
            put("pageSize", pageSize);
        }};
    }

    private HttpHeaders getHeaders(Object... args) {
        HttpHeaders headers = new HttpHeaders();
        headers.add("Content-Type", "application/json");
        if (args == null) {
            return headers;
        } else {
            if (args.length == 1) {
                headers.add("counter", String.valueOf(args[0]));
                return headers;
            }
            if (args.length == 2) {
                headers.add("counter", String.valueOf(args[0]));
                headers.add("rollback", String.valueOf(args[1]));
                return headers;
            }
        }
        return headers;
    }

    private HttpHeaders getReplicaHeaders() {
        HttpHeaders headers = getHeaders();
        headers.add("replica", String.valueOf(true));
        return headers;
    }

    private HttpHeaders getNotMainGroupHeaders() {
        HttpHeaders headers = getHeaders();
        headers.add("main", String.valueOf(false));
        return headers;
    }

    private void throwClientException(HttpClientErrorException e) {
        JSONObject json = new JSONObject(e.getResponseBodyAsString());
        if (e.getStatusCode() == HttpStatus.BAD_REQUEST) {
            throw new BadRequestException(json.getString("message"));
        }
        if (e.getStatusCode() == HttpStatus.NOT_FOUND) {
            throw new ResourceNotFoundException(json.getString("message"));
        }
    }

}