package by.webapp.kvstorage.repository;

import by.webapp.kvstorage.exception.DataBaseException;
import by.webapp.kvstorage.model.Collection;
import by.webapp.kvstorage.model.Document;
import by.webapp.kvstorage.util.Validator;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.core.annotation.Order;
import org.springframework.jdbc.core.BeanPropertyRowMapper;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowCountCallbackHandler;
import org.springframework.stereotype.Repository;
import org.springframework.transaction.annotation.Transactional;

import javax.annotation.PostConstruct;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Repository("documentRepository")
@Order(2)
public class DocumentRepository implements IDocumentRepository<Document> {

    private static final Logger logger = LogManager.getLogger(DocumentRepository.class);
    private final JdbcTemplate jdbcTemplate;
    private final CollectionRepository collectionRepository;
    private Map<Collection, List<Document>> documentsMap = new HashMap<>();
    private static final int MAX_ITEMS_COUNT = 100;
    private String documentName;

    @Autowired
    public DocumentRepository(JdbcTemplate jdbcTemplate, CollectionRepository collectionRepository) {
        this.jdbcTemplate = jdbcTemplate;
        this.collectionRepository = collectionRepository;
    }

    @PostConstruct
    private void init() {
        try {
            final Map<String, Collection> collections = collectionRepository.getPreparedCollections();
            for (Collection collection : collections.values()) {
                documentName = collection.getName();
                final String query = "SELECT COUNT (key) FROM " + documentName + ';';
                RowCountCallbackHandler countCallback = new RowCountCallbackHandler();
                jdbcTemplate.query(query, countCallback);
                final int countRows = countCallback.getRowCount();
                final String listQuery = "SELECT key, value FROM " + documentName + ';';
                List<Document> list = jdbcTemplate.query(
                        listQuery, new BeanPropertyRowMapper<>(Document.class));
                if (countRows > 1) {
                    int boundary = countRows > MAX_ITEMS_COUNT ?
                            MAX_ITEMS_COUNT : countRows;
                    list = list.subList(0, boundary);
                }
                documentsMap.put(collection, list);
            }
            logger.debug("DocumentRepository initialized");
        } catch (Exception e) {
            final String message = "Exception while document repository initializing.";
            logger.error(message);
            throw new DataBaseException(message, e);
        }
    }

    @Override
    public Document save(Document document) {
        documentName = Validator.getValidInput(documentName);
        final String validKey = Validator.getValidInput(document.getKey());
        final String value = Validator.getValidValue(document.getValue());
        final String query = "INSERT INTO " + documentName +
                " (key,value) VALUES (?,?)";
        try {
            jdbcTemplate.update(query, validKey, value);
            return findById(validKey);
        } catch (Exception e) {
            final String message = "Exception while document saving.";
            logger.error(message);
            throw new DataBaseException(message, e);
        }
    }

    @Override
    public Document findById(String key) {
        documentName = Validator.getValidInput(documentName);
        final String validKey = Validator.getValidInput(key);
        final String query = "SELECT key,value FROM " + documentName +
                " WHERE key = ?";
        try {
            return jdbcTemplate.queryForObject(query, new Object[]{validKey},
                    new BeanPropertyRowMapper<>(Document.class));
        } catch (Exception e) {
            final String message = "Exception while document getting.";
            logger.error(message);
            throw new DataBaseException(message, e);
        }
    }

    @Override
    public int update(String key, Document document) {
        documentName = Validator.getValidInput(documentName);
        final String validKey = Validator.getValidInput(key);
        final String value = Validator.getValidValue(document.getValue());
        final String query = "UPDATE " + documentName +
                " SET value=? WHERE key =?";
        try {
            return jdbcTemplate.update(query, value, validKey);
        } catch (Exception e) {
            final String message = "Exception while document updating.";
            logger.error(message);
            throw new DataBaseException(message, e);
        }
    }

    @Override
    public int delete(String key) {
        documentName = Validator.getValidInput(documentName);
        final String validKey = Validator.getValidInput(key);
        final String query = "DELETE FROM " + documentName +
                " WHERE key =?";
        try {
            return jdbcTemplate.update(query, validKey);
        } catch (Exception e) {
            final String message = "Exception while document deleting.";
            logger.error(message);
            throw new DataBaseException(message, e);
        }
    }

    @Override
    public List<Document> findAll() {
        documentName = Validator.getValidInput(documentName);
        final String query = "SELECT key, value FROM " + documentName;
        try {
            return jdbcTemplate.query(
                    query, new BeanPropertyRowMapper<>(Document.class));
        } catch (Exception e) {
            final String message = "Exception while receiving document list.";
            logger.error(message);
            throw new DataBaseException(message, e);
        }
    }

    public boolean isExist(String key) {
        documentName = Validator.getValidInput(documentName);
        final String sql = "SELECT count(*) FROM " + documentName + " WHERE key = ?";
        try {
            return jdbcTemplate.queryForObject(sql, new Object[]{key}, Integer.class) > 0;
        } catch (Exception e) {
            final String message = "Exception in document isExist() method.";
            logger.error(message);
            throw new DataBaseException(message, e);
        }
    }

    @Transactional
    public void createTable() {
        documentName = Validator.getValidInput(documentName);
        try {
            jdbcTemplate.update("CREATE TABLE " + documentName +
                    " (key VARCHAR(255) PRIMARY KEY, value text);");
        } catch (Exception e) {
            final String message = "Exception in document createTable() method.";
            logger.error(message);
            throw new DataBaseException(message, e);
        }
    }

    public void setDocumentName(String documentName) {
        this.documentName = Validator.getValidInput(documentName);
    }

    public Map<Collection, List<Document>> getPreparedDocuments() {
        return documentsMap;
    }

}