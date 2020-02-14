package by.webapp.kvstorage;

import by.webapp.kvstorage.exception.BadRequestException;
import by.webapp.kvstorage.model.Collection;
import by.webapp.kvstorage.service.CollectionService;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringRunner;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

@SpringBootTest
@RunWith(SpringRunner.class)
public class CollectionServiceTest {

    @Autowired
    CollectionService collectionService;

    @Before
    public void setUp() {
        collectionService.clean();
        Collection collection = new Collection();
        collection.setName("cats");
        collection.setAlgorithm("lRU");
        collection.setCacheLimit(10);
        collection.setJsonSchema("catsTestJsonSchema");
        collectionService.create(collection);
    }

    @Test
    public void testCreate() {
        Collection collection = new Collection();
        collection.setName("test");
        collection.setAlgorithm("LRU");
        collection.setCacheLimit(100);
        collection.setJsonSchema("testJsonSchema");
        assertNotNull(collectionService.create(collection));
    }

    @Test(expected = BadRequestException.class)
    public void testCreateShouldThrowException() {
        Collection collection = new Collection();
        collection.setName("bad'collectionName");
        collection.setAlgorithm("LRU");
        collection.setCacheLimit(100);
        collection.setJsonSchema("testJsonSchema");
        collectionService.create(collection);
    }

    @Test
    public void testGet() {
        assertTrue("cats".equalsIgnoreCase(
                (collectionService.get("cats")).getName()));
    }

    @Test
    public void testUpdate() {
        Collection collection = collectionService.get("cats");
        assertTrue("cats".equals(collection.getName())
                & "lru".equalsIgnoreCase(collection.getAlgorithm())
                & 10 == collection.getCacheLimit());
        collection.setAlgorithm("lFu");
        collection.setCacheLimit(100);
        collectionService.update("cats", collection);
        collection = collectionService.get("cats");
        assertTrue("LFU".equalsIgnoreCase(collection.getAlgorithm())
                & (100 == collection.getCacheLimit()));

    }

    @Test
    public void testDelete() {
        assertTrue(collectionService.delete("cats") > -1);
    }

    @Test
    public void testList() {
        assertNotNull(collectionService.list());
    }

    @Test(expected = BadRequestException.class)
    public void testIsValidAlgorithm() throws BadRequestException {
        Collection collection = new Collection();
        collection.setName("testAlgorithm");
        collection.setAlgorithm("Lru");
        collection.setCacheLimit(5);
        collection.setJsonSchema("testAlgorithmSchema");
        assertNotNull(collectionService.create(collection));
        collectionService.delete(collection.getName());
        collection.setAlgorithm("Slru");
        collectionService.create(collection);
    }

}