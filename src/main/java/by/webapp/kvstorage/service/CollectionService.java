package by.webapp.kvstorage.service;

import by.webapp.kvstorage.cache.CacheFactory;
import by.webapp.kvstorage.cache.ICache;
import by.webapp.kvstorage.exception.BadRequestException;
import by.webapp.kvstorage.exception.ResourceNotFoundException;
import by.webapp.kvstorage.model.Collection;
import by.webapp.kvstorage.repository.CollectionRepository;
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

@Service("collectionService")
@Transactional(readOnly = true)
@DependsOn("collectionRepository")
public class CollectionService implements GlobalService<Collection> {

    private static final Logger logger = LogManager.getLogger(CollectionService.class);
    private final CollectionRepository collectionRepository;
    private final DocumentService documentService;
    private ICache<String, Collection> collectionCache;
    private CacheFactory<Collection> cacheFactory = new CacheFactory<>();
    private final static String DEFAULT_CACHE_ALGORITHM = "LFU";
    private final static int DEFAULT_CACHE_CAPACITY = 1000;

    @PostConstruct
    public void init() {
        setCache(DEFAULT_CACHE_ALGORITHM, DEFAULT_CACHE_CAPACITY);
        final Map<String, Collection> preparedData = collectionRepository.getPreparedCollections();
        preparedData.forEach(collectionCache::put);
        logger.debug("CollectionService initialized");
    }

    @EventListener
    @Transactional
    @Order(5)
    public void onApplicationEvent(ContextRefreshedEvent event) {
        populateCollections();
        logger.debug("Collections populated");
    }

    @Autowired
    public CollectionService(CollectionRepository collectionRepository,
                             DocumentService documentService) {
        this.collectionRepository = collectionRepository;
        this.documentService = documentService;
    }

    @Override
    @Transactional
    public Collection create(Collection collection) throws BadRequestException {
        final String collectionName = collection.getName();
        if (!isAbsent(collectionName)) {
            collectionCache.put(collectionName, collection);
            final String message = "Error: an attempt to create an existing collection.";
            logger.error(message);
            throw new BadRequestException(message);
        }
        setCache(collection.getAlgorithm(), collection.getCacheLimit());
        documentService.setCollection(collection);
        documentService.setDocumentName(collectionName);
        documentService.createTable();
        final Collection createdCollection = collectionRepository.create(collection);
        collectionCache.put(collectionName, createdCollection);
        return createdCollection;
    }

    @Override
    public Collection get(String collectionName) throws ResourceNotFoundException {
        if (isAbsent(collectionName)) {
            final String message = "Error: an attempt to get non-existing collection.";
            logger.error(message);
            throw new ResourceNotFoundException(message);
        }
        Collection collection;
        if (collectionCache.contains(collectionName)) {
            collection = collectionCache.get(collectionName);
        } else {
            collection = collectionRepository.get(collectionName);
            collectionCache.put(collectionName, collection);
        }
        return collection;
    }

    @Override
    @Transactional
    public int update(String collectionName, Collection collection) throws ResourceNotFoundException {
        if (isAbsent(collectionName)) {
            final String message = "Error: attempt to update non-existing collection.";
            logger.error(message);
            throw new ResourceNotFoundException(message);
        }
        collectionCache.remove(collectionName);
        collectionCache.put(collectionName, collection);
        return collectionRepository.update(collectionName, collection);
    }

    @Override
    @Transactional
    public int delete(String collectionName) throws RuntimeException {
        final String safeCollectionName = Validator.getValidInput(collectionName);
        if (isAbsent(safeCollectionName)) {
            throw new ResourceNotFoundException(
                    "Error: attempt to delete non-existing collection.");
        }
        collectionCache.remove(safeCollectionName);
        return collectionRepository.delete(safeCollectionName);
    }

    @Override
    public List<Collection> list() {
        return collectionRepository.list();
    }

    @Transactional
    public void clean() {
        collectionRepository.clean();
    }

    public boolean isAbsent(String collectionName) {
        return !collectionRepository.isExist(collectionName);
    }

    private void setCache(String algorithmName, Integer cacheLimit) {
        collectionCache = cacheFactory.create(algorithmName, cacheLimit);
    }

    private void populateCollections() {
        collectionRepository.getPreparedCollections().values().stream()
                .filter(collection -> isAbsent(collection.getName())).forEach(this::create);
    }

}