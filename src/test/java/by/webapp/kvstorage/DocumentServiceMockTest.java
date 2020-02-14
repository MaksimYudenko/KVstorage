package by.webapp.kvstorage;

import by.webapp.kvstorage.model.Document;
import by.webapp.kvstorage.service.DocumentService;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertSame;
import static org.mockito.Mockito.*;

@RunWith(MockitoJUnitRunner.class)
public class DocumentServiceMockTest {

    @Mock
    DocumentService documentService;
    private Document document;


    @Before
    public void setUp() {
        document = new Document();
        document.setKey("firstMock");
        document.setValue("testSchema");
    }

    @Test
    public void testCreate() {
        documentService.create(document);
        verify(documentService, atLeastOnce()).create(document);
    }

    @Test
    public void testGet() {
        when(documentService.get("name")).thenReturn(document);
        assertSame(documentService.get("name"), document);
    }

    @Test
    public void testUpdate() {
        when(documentService.update("name", document)).thenReturn(1);
        assertEquals(1, documentService.update("name", document));
    }

    @Test
    public void testDelete() {
        when(documentService.delete("name")).thenReturn(1);
        assertEquals(1, documentService.delete("name"));
    }

    @Test
    public void testList() {
        documentService.list();
        verify(documentService, atLeastOnce()).list();
    }

}