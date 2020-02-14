package by.webapp.kvstorage;

import by.webapp.kvstorage.exception.BadRequestException;
import by.webapp.kvstorage.model.Collection;
import by.webapp.kvstorage.model.Document;
import by.webapp.kvstorage.service.CollectionService;
import by.webapp.kvstorage.service.DocumentService;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringRunner;

import static org.junit.Assert.*;

@SpringBootTest
@RunWith(SpringRunner.class)
public class DocumentServiceTest {

    @Autowired
    CollectionService collectionService;
    @Autowired
    DocumentService documentService;

    @Before
    public void setUp() {
        collectionService.clean();
        Collection collection = new Collection();
        collection.setName("cats");
        collection.setAlgorithm("lRU");
        collection.setCacheLimit(10);
        collection.setJsonSchema("{\"definitions\": {},\"$schema\": \"http://json-schema.org/draft-07/schema#\",\"$id\": \"http://example.com/root.json\",\"type\": \"object\",\"title\": \"The Root Schema\",\"required\": [\"name\",\"age\"],\"properties\": {\"name\": {\"$id\": \"#/properties/name\",\"type\": \"string\",\"title\": \"The Name Schema\",\"default\": \"\",\"examples\": [\"Jack\"],\"pattern\": \"^(.*)$\"},\"age\": {\"$id\": \"#/properties/age\",\"type\": \"integer\",\"title\": \"The Age Schema\",\"default\": 0,\"examples\": [3]}}}");
        collectionService.create(collection);
        Document document = new Document();
        documentService.setDocumentName(collection.getName());
        document.setKey("cat1");
        document.setValue("{\n\"name\":\"Moorka\",\n\"age\":3\n}");
        documentService.create(document);
        document.setKey("cat2");
        document.setValue("{\n\"name\":\"Barsik\",\n\"age\":5\n}");
        documentService.create(document);
    }

    @Test
    public void testCreate() {
        Document document = new Document();
        document.setKey("angryCat");
        document.setValue("{\n\"name\":\"Tom\",\n\"age\":15\n}");
        assertNotNull(documentService.create(document));
    }

    @Test
    public void testGet() {
        assertTrue((documentService.get("cat2")).getValue().contains("Barsik"));
    }

    @Test
    public void testUpdate() {
        Document document = documentService.get("cat1");
        assertTrue(document.getValue().contains("3"));
        document.setValue("{\n\"name\":\"Tom\",\n\"age\":15\n}");
        documentService.update("cat1", document);
        assertTrue(document.getValue().contains("15"));
    }

    @Test
    public void testDelete() {
        assertEquals(1, documentService.delete("cat2"));
    }

    @Test(expected = BadRequestException.class)
    public void testIsValidToJsonSchema() {
        Document document = new Document();
        document.setKey("angryCat");
        document.setValue("{\n\"name\":\"Tom\",\n\"age\":\"15\"\n}");
        documentService.create(document);
    }

}