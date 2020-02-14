package by.webapp.kvstorage;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.http.MediaType;

import java.io.IOException;
import java.nio.charset.StandardCharsets;

class TestUtil {

    static final String SCHEMA = "{\"definitions\": {},\"$schema\": \"http://json-schema.org/draft-07/schema#\",\"$id\": \"http://example.com/root.json\",\"type\": \"object\",\"title\": \"The Root Schema\",\"required\": [\"name\",\"age\"],\"properties\": {\"name\": {\"$id\": \"#/properties/name\",\"type\": \"string\",\"title\": \"The Name Schema\",\"default\": \"\",\"examples\": [\"mouse\"],\"pattern\": \"^(.*)$\"},\"age\": {\"$id\": \"#/properties/age\",\"type\": \"integer\",\"title\": \"The Age Schema\",\"default\": 0,\"examples\": [5]}}}";
    static final MediaType APPLICATION_JSON_UTF8 = new MediaType(
            MediaType.APPLICATION_JSON.getType(),
            MediaType.APPLICATION_JSON.getSubtype(),
            StandardCharsets.UTF_8);

    static byte[] convertObjectToJsonBytes(Object object) throws IOException {
        ObjectMapper mapper = new ObjectMapper();
        mapper.setSerializationInclusion(JsonInclude.Include.NON_NULL);
        return mapper.writeValueAsBytes(object);
    }

}