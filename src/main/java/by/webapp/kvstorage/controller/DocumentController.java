package by.webapp.kvstorage.controller;

import by.webapp.kvstorage.exception.DataBaseException;
import by.webapp.kvstorage.exception.FailedException;
import by.webapp.kvstorage.model.Document;
import by.webapp.kvstorage.service.DistributedService;
import by.webapp.kvstorage.service.DocumentService;
import by.webapp.kvstorage.service.RollbackService;
import by.webapp.kvstorage.util.NodeLoader;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpMethod;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.client.ResourceAccessException;

import javax.validation.Valid;
import java.util.List;
import java.util.stream.Collectors;

@RestController
@RequestMapping(
        value = "/collections/{collectionId}", produces = MediaType.APPLICATION_JSON_VALUE)
public class DocumentController {

    private static final Logger logger = LogManager.getLogger(DocumentController.class);
    private static final String INITIAL_PAGE = "0";
    private static final String INITIAL_PAGE_SIZE = "10";
    private final DocumentService documentService;
    private final DistributedService distributedService;
    private final RollbackService rollbackService;

    @Autowired
    public DocumentController(DocumentService documentService,
                              DistributedService distributedService,
                              RollbackService rollbackService) {
        this.documentService = documentService;
        this.distributedService = distributedService;
        this.rollbackService = rollbackService;
        logger.debug("DocumentController initialized");
    }

    @PostMapping
    @ResponseStatus(HttpStatus.CREATED)
    public Document create(
            @PathVariable String collectionId,
            @Valid @RequestBody Document document,
            @RequestHeader(name = "counter", required = false,
                    defaultValue = "0") int counter,
            @RequestHeader(name = "rollback", required = false,
                    defaultValue = "false") boolean shouldRollBack) {
        String documentId = document.getKey();
        Document result;
        if (distributedService.isMyGroup(collectionId + "/" + documentId)) {
            try {
                documentService.setDocumentName(collectionId);
                result = documentService.create(document);
                distributedService.sendPost(
                        document, counter, shouldRollBack, collectionId, documentId);
                return result;
            } catch (DataBaseException | ResourceAccessException e) {
                if (e instanceof ResourceAccessException) {
                    logger.warn("Starting rollback for POST request in current node");
                    documentService.delete(documentId);
                } else {
                    logger.error("Problem with data base in  ", e);
                }
                rollbackService.rollback(counter, collectionId, documentId);
                throw new FailedException("Problem with DB: ", e);
            }
        } else {
            return distributedService.redirectPost(document, collectionId, documentId);
        }
    }

    @GetMapping("/{documentId}")
    public Document get(
            @PathVariable String collectionId, @PathVariable String documentId,
            @RequestHeader(name = "replica", required = false,
                    defaultValue = "false") boolean isReplica) {
        documentService.setDocumentName(collectionId);
        Document document;
        if (distributedService.isMyGroup(collectionId + "/" + documentId)) {
            if (documentService.isExist(documentId)) {
                try {
                    document = documentService.get(documentId);
                } catch (DataBaseException e) {
                    logger.error("Problem with Data Base in " +
                            NodeLoader.thisNode.getName(), e);
                    if (!isReplica) {
                        return (Document) distributedService.sendGet(
                                Document.class, collectionId, documentId);
                    } else {
                        throw new FailedException("Problem with Data Base", e);
                    }
                }
            } else {
                document = (Document) distributedService.
                        sendGet(Document.class, collectionId, documentId);
            }
        } else {
            logger.debug("Document " + documentId + " returned.");
            document = (Document) distributedService.redirectGet(
                    Document.class, collectionId, documentId);
        }
        return document;
    }

    @PutMapping("/{documentId}")
    public int update(
            @PathVariable String collectionId, @PathVariable String documentId,
            @Valid @RequestBody Document document,
            @RequestHeader(name = "counter", required = false
                    , defaultValue = "0") int counter,
            @RequestHeader(name = "rollback", required = false,
                    defaultValue = "false") boolean shouldRollBack)
            throws CloneNotSupportedException {
        documentService.setDocumentName(collectionId);
        int flag = 0;
        if (distributedService.isMyGroup(collectionId + "/" + documentId)) {
            Document documentOldValue = null;
            try {
                documentOldValue = documentService.get(documentId).clone();
                flag = documentService.update(documentId, document);
                distributedService.sendUpdate(
                        document, counter, shouldRollBack, collectionId, documentId);
            } catch (DataBaseException | ResourceAccessException e) {
                if (shouldRollBack) {
                    logger.error("Problem with rollback. The application doesn't work correctly");
                    throw new FailedException("Problem with rollback.", e);
                }
                if (e instanceof ResourceAccessException) {
                    documentService.update(documentId, documentOldValue);
                } else {
                    logger.error("Problem with Data Base in  " +
                            NodeLoader.thisNode.getName(), e);
                }
                rollbackService.rollback(documentOldValue, counter,
                        HttpMethod.PUT, collectionId, documentId);
            }
        } else {
            logger.debug("Document " + documentId + " updated.");
            distributedService.redirectUpdate(document, collectionId, documentId);
            return 1;
        }
        return flag;
    }

    @DeleteMapping("/{documentId}")
    public int delete(
            @PathVariable String collectionId, @PathVariable String documentId,
            @RequestHeader(name = "counter", required = false
                    , defaultValue = "0") int counter,
            @RequestHeader(name = "rollback", required = false,
                    defaultValue = "false") boolean shouldRollBack)
            throws CloneNotSupportedException {
        documentService.setDocumentName(collectionId);
        int flag = -1;
        if (distributedService.isMyGroup(collectionId + "/" + documentId)) {
            Document documentOldValue = null;
            try {
                documentOldValue = documentService.get(documentId).clone();
                flag = documentService.delete(documentId);
                distributedService.sendDelete(
                        counter, shouldRollBack, collectionId, documentId);
            } catch (DataBaseException | ResourceAccessException e) {
                if (shouldRollBack) {
                    throw new FailedException("Problem with rollback.", e);
                }
                if (e instanceof ResourceAccessException) {
                    documentService.create(documentOldValue);
                } else {
                    logger.error("Problem with Data Base in  " +
                            NodeLoader.thisNode.getName(), e);
                }
                rollbackService.rollback(documentOldValue, counter,
                        HttpMethod.DELETE, collectionId);
            }
        } else {
            logger.debug("Document " + documentId + " deleted.");
            distributedService.redirectDelete(collectionId, documentId);
            flag = 1;
        }
        return flag;
    }

    @GetMapping
    public List<Document> getAll(
            @PathVariable String collectionId,
            @RequestParam(name = "page", required = false,
                    defaultValue = INITIAL_PAGE) int page,
            @RequestParam(name = "pageSize", required = false,
                    defaultValue = INITIAL_PAGE_SIZE) int pageSize,
            @RequestHeader(name = "main", required = false,
                    defaultValue = "true") boolean isCurrentGroup,
            @RequestHeader(name = "replica", required = false,
                    defaultValue = "false") boolean isReplica) {
        {
            documentService.setDocumentName(collectionId);
            List<Document> documentList;
            try {
                documentList = documentService.list();
            } catch (DataBaseException e) {
                logger.error("Problem with Data Base in  " +
                        NodeLoader.thisNode.getName(), e);
                if (isReplica) {
                    throw new FailedException("Problem with Data Base", e);
                }
                documentList = distributedService.sendListToReplica(
                        page, pageSize, collectionId)
                        .stream().map((obj) -> (Document) obj).collect(Collectors.toList());
            }
            if (isCurrentGroup) {
                documentList = distributedService.distributeDocumentList(
                        page, pageSize, collectionId, documentList);
            }
            logger.debug("Documents list returned.");
            return documentList;
        }
    }

}