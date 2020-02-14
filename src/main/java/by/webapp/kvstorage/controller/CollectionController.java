package by.webapp.kvstorage.controller;

import by.webapp.kvstorage.exception.DataBaseException;
import by.webapp.kvstorage.exception.FailedException;
import by.webapp.kvstorage.model.Collection;
import by.webapp.kvstorage.service.CollectionService;
import by.webapp.kvstorage.service.DistributedService;
import by.webapp.kvstorage.service.RollbackService;
import by.webapp.kvstorage.util.NodeLoader;
import by.webapp.kvstorage.util.Validator;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.client.ResourceAccessException;

import javax.validation.Valid;
import java.util.List;
import java.util.Objects;

@RestController
@RequestMapping(
        value = "/collections", produces = MediaType.APPLICATION_JSON_VALUE)
public class CollectionController {

    private static final Logger logger = LogManager.getLogger(CollectionController.class);
    private static final String INITIAL_PAGE = "0";
    private static final String INITIAL_PAGE_SIZE = "10";
    private final CollectionService collectionService;
    private final DistributedService distributedService;
    private final RollbackService rollbackService;

    @Autowired
    public CollectionController(CollectionService collectionService,
                                DistributedService distributedService,
                                RollbackService rollbackService) {
        this.collectionService = collectionService;
        this.distributedService = distributedService;
        this.rollbackService = rollbackService;
        logger.debug("CollectionController initialized");
    }

    @PostMapping
    @ResponseStatus(HttpStatus.CREATED)
    public Collection create(
            @Valid @RequestBody Collection collection,
            @RequestHeader(name = "counter", required = false,
                    defaultValue = "0") int counter,
            @RequestHeader(name = "rollback", required = false,
                    defaultValue = "false") boolean shouldRollBack) {
        final String collectionId = collection.getName();
        String nodeName = NodeLoader.thisNode.getName();
        Collection result;
        try {
            result = collectionService.create(collection);
            distributedService.distributeCreating(collection, counter, shouldRollBack);
        } catch (DataBaseException | ResourceAccessException e) {
            if (e instanceof ResourceAccessException) {
                logger.debug("RollBack while creating collection in " + nodeName);
                collectionService.delete(collectionId);
            } else {
                logger.error("DataBase error in " + nodeName, e);
            }
            rollbackService.rollback(counter, collectionId);
            final String message = "Exception in CollectionController " +
                    "while create collection in " + nodeName;
            logger.debug(message);
            throw new FailedException(message, e);
        }
        logger.debug("Collection " + collectionId + " created.");
        return result;
    }

    @PutMapping("/{collectionId}")
    public int update(
            @PathVariable String collectionId,
            @Valid @RequestBody Collection collection,
            @RequestHeader(name = "counter", required = false, defaultValue = "0")
                    int counter,
            @RequestHeader(name = "rollback", required = false, defaultValue = "false")
                    boolean shouldRollBack) {
        Collection currentCollection = null;
        String nodeName = NodeLoader.thisNode.getName();
        int flag;
        try {
            currentCollection = collectionService.get(collectionId).clone();
            flag = collectionService.update(collectionId, collection);
            distributedService.distributeUpdating(
                    collection, counter, shouldRollBack, collectionId);
        } catch (DataBaseException | ResourceAccessException | CloneNotSupportedException e) {
            if (shouldRollBack) {
                final String message = "Error while updating collection in " + nodeName;
                logger.fatal(message);
                throw new FailedException(message, e);
            }
            if (e instanceof ResourceAccessException) {
                collectionService.update(collectionId, currentCollection);
            } else {
                logger.error("DataBase error in " + nodeName, e);
            }
            rollbackService.rollback(counter, collectionId);
            final String message = "Exception in CollectionController " +
                    "while updating collection in " + nodeName;
            logger.debug(message);
            throw new FailedException(message, e);
        }
        logger.debug("Collection updated in " + nodeName);
        return flag;
    }

    @DeleteMapping("/{collectionId}")
    public int delete(
            @PathVariable String collectionId,
            @RequestHeader(name = "counter", required = false, defaultValue = "0")
                    int counter,
            @RequestHeader(name = "rollback", required = false, defaultValue = "false")
                    boolean shouldRollBack) {
        String nodeName = NodeLoader.thisNode.getName();
        Collection currentCollection = null;
        int flag;
        try {
            currentCollection = collectionService.get(collectionId).clone();
            flag = collectionService.delete(collectionId);
            distributedService.distributeDeleting(counter, shouldRollBack, collectionId);
        } catch (DataBaseException | ResourceAccessException | CloneNotSupportedException e) {
            if (shouldRollBack) {
                final String message = "Error while deleting collection in " + nodeName;
                logger.fatal(message);
                throw new FailedException(message, e);
            }
            if (e instanceof ResourceAccessException) {
                logger.debug("RollBack while deleting collection in " + nodeName);
                collectionService.create(Objects.requireNonNull(currentCollection));
            } else {
                logger.error("DataBase error in " + nodeName, e);
            }
            assert currentCollection != null;
            rollbackService.rollback(counter, currentCollection.getName());
            final String message = "Exception in CollectionController while" +
                    " deleting collection in " + nodeName;
            logger.debug(message);
            throw new FailedException(message, e);
        }
        logger.debug("Collection deleted in " + nodeName);
        return flag;
    }

    @DeleteMapping
    public int deleteAll(
            @RequestHeader(name = "counter", required = false, defaultValue = "0")
                    int counter,
            @RequestHeader(name = "rollback", required = false, defaultValue = "false")
                    boolean shouldRollBack) {
        List<Collection> currentCollectionList = null;
        String nodeName = NodeLoader.thisNode.getName();
        int flag;
        try {
            currentCollectionList = collectionService.list();
            collectionService.clean();
            distributedService.distributeDeleting(counter, shouldRollBack);
            flag = 1;
        } catch (DataBaseException | ResourceAccessException e) {
            if (shouldRollBack) {
                final String message =
                        "Error while cleaning collections in " + nodeName;
                logger.fatal(message);
                throw new FailedException(message, e);
            }
            if (e instanceof ResourceAccessException) {
                logger.debug("RollBack while clean collection in " + nodeName);
                assert currentCollectionList != null;
                currentCollectionList.forEach(collectionService::create);
            } else {
                logger.error("DataBase error in " + nodeName, e);
            }
            final String message = "Exception in CollectionController " +
                    "while cleaning collections in " + nodeName;
            logger.debug(message);
            throw new FailedException(message, e);
        }
        logger.debug("All collections deleted in " + nodeName);
        return flag;
    }

    @GetMapping
    public List<Collection> getAll(
            @RequestParam(name = "page", required = false,
                    defaultValue = INITIAL_PAGE) int offSet,
            @RequestParam(name = "pageSize", required = false,
                    defaultValue = INITIAL_PAGE_SIZE) int pageSize,
            @RequestHeader(name = "replica", required = false,
                    defaultValue = "false") boolean isReplica) {
        String nodeName = NodeLoader.thisNode.getName();
        List<Collection> collectionList;
        try {
            collectionList = collectionService.list();
        } catch (DataBaseException e) {
            final String message = "DataBase error in CollectionController" +
                    " while getting collection list in " + nodeName;
            logger.error(message);
            if (!isReplica) {
                final String replicaMessage = "DataBase error in CollectionController " +
                        "while getting collection list in " + nodeName +
                        ".\n Thus empty list returned.\n Exception cause is:\n" + e.getMessage();
                logger.error(replicaMessage);
                throw new FailedException(replicaMessage, e);
            }
            collectionList = distributedService.distributeGettingList(offSet, pageSize);
        }
        logger.debug("Collections list returned in " + nodeName);
        return Validator.getCollectionSubList(collectionList, offSet, pageSize);
    }

}