package by.webapp.kvstorage.repository;

import by.webapp.kvstorage.exception.DataBaseException;
import by.webapp.kvstorage.model.Collection;
import by.webapp.kvstorage.util.Validator;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.core.annotation.Order;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowCountCallbackHandler;
import org.springframework.stereotype.Repository;

import javax.annotation.PostConstruct;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Repository("collectionRepository")
@Order(0)
public class CollectionRepository {

    private static final Logger logger = LogManager.getLogger(CollectionRepository.class);
    private final JdbcTemplate jdbcTemplate;
    private final ICollectionRepository iCollectionRepository;
    private Map<String, Collection> collections = new HashMap<>();
    private static final int MAX_ITEMS_COUNT = 1000;

    @Autowired
    public CollectionRepository(JdbcTemplate jdbcTemplate,
                                ICollectionRepository iCollectionRepository) {
        this.jdbcTemplate = jdbcTemplate;
        this.iCollectionRepository = iCollectionRepository;
    }

    @PostConstruct
    private void init() {
        final String query = "SELECT COUNT (name) FROM collections;";
        try {
            RowCountCallbackHandler countCallback = new RowCountCallbackHandler();
            jdbcTemplate.query(query, countCallback);
            final int countRows = countCallback.getRowCount();
            List<Collection> list;
            if (countRows == 1) {
                list = iCollectionRepository.findAll();
            } else {
                int boundary = Math.min(countRows, MAX_ITEMS_COUNT);
                list = iCollectionRepository.findAll().subList(0, boundary);
            }
            list.forEach(collection -> collections.put(collection.getName(), collection));
            logger.debug("CollectionRepository initialized");
        } catch (Exception e) {
            final String message = "Exception while collection repository initializing.";
            logger.error(message);
            throw new DataBaseException(message, e);
        }
    }

    public Collection create(Collection collection) {
        try {
            return iCollectionRepository.saveAndFlush(collection);
        } catch (Exception e) {
            final String message = "Exception while collection creating.";
            logger.error(message);
            throw new DataBaseException(message, e);
        }
    }

    public Collection get(String collectionName) {
        try {
            return iCollectionRepository.getOne(Validator.getValidInput(collectionName));
        } catch (Exception e) {
            final String message = "Exception while collection getting.";
            logger.error(message);
            throw new DataBaseException(message, e);
        }
    }

    public int update(String collectionName, Collection collection) {
        final String validCollectionName = Validator.getValidInput(collectionName);
        final String algorithmName = collection.getAlgorithm();
        final Integer cacheLimit = collection.getCacheLimit();
        try {
            final String query = "UPDATE collections SET " +
                    "algorithm = ?, cache_limit = ? WHERE name = ?";
            return jdbcTemplate.update(query, algorithmName, cacheLimit, validCollectionName);
        } catch (Exception e) {
            final String message = "Exception while collection updating.";
            logger.error(message);
            throw new DataBaseException(message, e);
        }
    }

    public int delete(String collectionName) {
        final String validCollectionName = Validator.getValidInput(collectionName);
        try {
            iCollectionRepository.deleteById(validCollectionName);
            return jdbcTemplate.update("DROP TABLE " + validCollectionName);
        } catch (Exception e) {
            final String message = "Exception while collection deleting.";
            logger.error(message);
            throw new DataBaseException(message, e);
        }
    }

    public List<Collection> list() {
        try {
            return iCollectionRepository.findAll();
        } catch (Exception e) {
            final String message = "Exception while receiving collection list.";
            logger.error(message);
            throw new DataBaseException(message, e);
        }
    }

    public boolean isExist(String collectionName) {
        return iCollectionRepository.existsById(Validator.getValidInput(collectionName));
    }

    public void clean() {
        try {
            final String query = "DROP SCHEMA public CASCADE;\n" +
                    "CREATE SCHEMA public;" +
                    "CREATE TABLE IF NOT EXISTS collections\n" +
                    "(\n" +
                    "  name        varchar(255) not null primary key,\n" +
                    "  algorithm   varchar(255) not null,\n" +
                    "  cache_limit integer      not null,\n" +
                    "  json_schema text         not null\n" +
                    ");";
            jdbcTemplate.update(query);
        } catch (Exception e) {
            final String message = "Exception while cleaning collection.";
            logger.error(message);
            throw new DataBaseException(message, e);
        }
    }

    public int size() {
        final String query = "SELECT COUNT (name) FROM collections;";
        RowCountCallbackHandler countCallback = new RowCountCallbackHandler();
        jdbcTemplate.query(query, countCallback);
        return countCallback.getRowCount();
    }

    public Map<String, Collection> getPreparedCollections() {
        return collections;
    }

}