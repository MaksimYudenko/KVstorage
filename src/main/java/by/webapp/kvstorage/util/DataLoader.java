package by.webapp.kvstorage.util;

import by.webapp.kvstorage.model.Collection;
import by.webapp.kvstorage.service.CollectionService;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.core.annotation.Order;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;

@Component
@Order(10)
public class DataLoader implements ApplicationRunner {

    private static final Logger logger = LogManager.getLogger(DataLoader.class);
    private final CollectionService collectionService;

    @Autowired
    public DataLoader(CollectionService collectionService) {
        this.collectionService = collectionService;
    }

    @PostConstruct
    public void init() {
        collectionService.clean();
        logger.debug("DataLoader cleaned all data.");
    }

    @Override
    public void run(ApplicationArguments args) {
        Collection collection = new Collection();
        collection.setName("startup");
        collection.setAlgorithm("LFU");
        collection.setCacheLimit(1000);
        collection.setJsonSchema("jsonSchema");
        if (collectionService.isAbsent(collection.getName())) {
            collectionService.create(collection);
        }
        logger.debug("Data loader run.");
    }

}