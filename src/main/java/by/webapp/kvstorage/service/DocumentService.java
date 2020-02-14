package by.webapp.kvstorage.service;

import by.webapp.kvstorage.cache.CacheFactory;
import by.webapp.kvstorage.cache.ICache;
import by.webapp.kvstorage.exception.BadRequestException;
import by.webapp.kvstorage.exception.ResourceNotFoundException;
import by.webapp.kvstorage.model.Collection;
import by.webapp.kvstorage.model.Document;
import by.webapp.kvstorage.repository.DocumentRepository;
import by.webapp.kvstorage.util.Validator;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.DependsOn;
import org.springframework.context.event.ContextRefreshedEvent;
import org.springframework.context.event.EventListener;
import org.springframework.core.annotation.Order;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import javax.annotation.PostConstruct;
import java.util.List;
import java.util.Map;

@Service("documentService")
@Transactional(readOnly = true)
@DependsOn("documentRepository")
public class DocumentService implements GlobalService<Document> {

    private static final Logger logger = LogManager.getLogger(DocumentService.class);
    private final DocumentRepository documentRepository;
    private CacheFactory<String> cacheFactory = new CacheFactory<>();
    private ICache<String, String> documentCache;
    private Collection collection;
    private String documentName;
    private Map<Collection, List<Document>> preparedDocuments;

    @PostConstruct
    public void init() {
        preparedDocuments = documentRepository.getPreparedDocuments();
        logger.debug("DocumentService initialized");
    }

    @EventListener
    @Transactional
    @Order(6)
    public void onApplicationEvent(ContextRefreshedEvent event) {
        populateDocuments();
        logger.debug("Documents populated");
    }

    @Autowired
    public DocumentService(DocumentRepository documentRepository) {
        this.documentRepository = documentRepository;
    }

    @Override
    @Transactional
    public Document create(Document document) throws BadRequestException {
        for (Collection entry : preparedDocuments.keySet()) {
            if (entry.getName().equalsIgnoreCase(documentName)) {
                collection = entry;
            }
        }
        setCache(collection.getAlgorithm(), collection.getCacheLimit());
        final String key = document.getKey();
        final String value = document.getValue();
        if (isExist(key)) {
            documentCache.put(key, value);
            final String message = "Error: an attempt to create an existing document.";
            logger.error(message);
            throw new BadRequestException(message);
        }
        if (Validator.isInvalidToJsonSchema(value, collection.getJsonSchema())) {
            final String message = "Error: value does not match json schema.";
            logger.error(message);
            throw new BadRequestException(message);
        }
        documentCache.put(key, value);
        return documentRepository.save(document);
    }

    @Override
    public Document get(String key) throws ResourceNotFoundException {
        if (documentName == null) {
            final String message = "Error: an attempt to get document from non-existing collection.";
            logger.error(message);
            throw new ResourceNotFoundException(message);
        }
        if (!isExist(key)) {
            final String message = "Error: an attempt to get non-existing document.";
            logger.error(message);
            throw new ResourceNotFoundException(message);
        }
        Document document = new Document();
        if (documentCache.contains(key)) {
            document.setKey(key);
            document.setValue(documentCache.get(key));
        } else {
            document = documentRepository.findById(key);
            documentCache.put(key, document.getValue());
        }
        return document;
    }

    @Override
    @Transactional
    public int update(String key, Document document) throws RuntimeException {
        if (!isExist(key)) {
            final String message = "Error: an attempt to update non-existing document.";
            logger.error(message);
            throw new ResourceNotFoundException(message);
        }
        final String newValue = document.getValue();
        for (Collection entry : preparedDocuments.keySet()) {
            if (entry.getName().equalsIgnoreCase(documentName)) {
                collection = entry;
            }
        }
        if (Validator.isInvalidToJsonSchema(newValue, collection.getJsonSchema())) {
            final String message = "Error: value does not match json schema.";
            logger.error(message);
            throw new BadRequestException(message);
        }
        documentCache.remove(key);
        documentCache.put(key, newValue);
        return documentRepository.update(key, document);
    }

    @Override
    @Transactional
    public int delete(String key) throws ResourceNotFoundException {
        if (!isExist(key)) {
            final String message = "Error: attempt to delete non-existing document";
            logger.error(message);
            throw new ResourceNotFoundException(message);
        }
        documentCache.remove(key);
        return documentRepository.delete(key);
    }

    @Override
    public List<Document> list() {
        if (documentName == null) {
            final String message = "Error: an attempt to get documents from non-existing collections.";
            logger.error(message);
            throw new ResourceNotFoundException(message);
        }
        return documentRepository.findAll();
    }

    public boolean isExist(String key) {
        return documentRepository.isExist(key);
    }

    void createTable() {
        documentRepository.createTable();
    }

    public void setCollection(Collection collection) {
        this.collection = collection;
    }

    public void setDocumentName(String documentName) {
        final String safeName = Validator.getValidInput(documentName);
        this.documentName = safeName;
        documentRepository.setDocumentName(safeName);
    }

    private void setCache(String algorithmName, Integer cacheLimit) {
        documentCache = cacheFactory.create(algorithmName, cacheLimit);
    }

    private void populateDocuments() {
        for (Map.Entry<Collection, List<Document>> entry
                : preparedDocuments.entrySet()) {
            final String collName = entry.getKey().getName();
            this.collection = entry.getKey();
            setDocumentName(collName);
            setCache(collection.getAlgorithm(), collection.getCacheLimit());
            List<Document> docList = entry.getValue();
            if (docList.size() > 0 & collection != null) {
                docList.stream()
                        .filter(document -> !isExist(document.getKey()))
                        .forEach(this::create);
                docList.stream()
                        .filter(document -> !documentCache.contains(document.getKey()))
                        .forEach((doc) -> documentCache.put(doc.getKey(), doc.getValue()));
            }
        }
    }

}