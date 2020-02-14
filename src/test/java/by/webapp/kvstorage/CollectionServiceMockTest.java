package by.webapp.kvstorage;

import by.webapp.kvstorage.model.Collection;
import by.webapp.kvstorage.service.CollectionService;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertSame;
import static org.mockito.Mockito.*;

@RunWith(MockitoJUnitRunner.class)
public class CollectionServiceMockTest {

    @Mock
    CollectionService collectionService;
    private Collection collection;

    @Before
    public void setUp() {
        collection = new Collection();
        collection.setName("mock");
        collection.setAlgorithm("lRU");
        collection.setCacheLimit(10);
        collection.setJsonSchema("testSchema");
    }

    @Test
    public void testCreate() {
        collectionService.create(collection);
        verify(collectionService, atLeastOnce()).create(collection);
    }

    @Test
    public void testGet() {
        when(collectionService.get("name")).thenReturn(collection);
        assertSame(collectionService.get("name"), collection);
    }

    @Test
    public void testUpdate() {
        when(collectionService.update("name", collection)).thenReturn(1);
        assertEquals(1, collectionService.update("name", collection));
    }

    @Test
    public void testDelete() {
        when(collectionService.delete("name")).thenReturn(1);
        assertEquals(1, collectionService.delete("name"));
    }

    @Test
    public void testList() {
        collectionService.list();
        verify(collectionService, atLeastOnce()).list();
    }

}