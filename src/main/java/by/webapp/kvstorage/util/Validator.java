package by.webapp.kvstorage.util;

import by.webapp.kvstorage.exception.BadRequestException;
import by.webapp.kvstorage.model.Collection;
import by.webapp.kvstorage.model.Document;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.everit.json.schema.loader.SchemaLoader;
import org.json.JSONObject;
import org.json.JSONTokener;
import org.owasp.esapi.ESAPI;
import org.owasp.esapi.errors.ValidationException;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

public class Validator {

    private static final Logger logger = LogManager.getLogger(Validator.class);

    private final static int MAX_LENGTH = 250;
    private static final int VALUE_FACTOR = 10;

    public static String getValidInput(String input) {
        String resultInput;
        try {
            resultInput = ESAPI.validator().getValidInput(
                    "input string", input,
                    "NameCharacters", MAX_LENGTH, false);
        } catch (ValidationException e) {
            final String message = "Validation error: [" + input + "]," + e.getMessage();
            logger.error(message);
            throw new BadRequestException(message);
        }
        return resultInput;

    }

    public static String getValidValue(String input) {
        String resultValue;
        try {
            resultValue = ESAPI.validator().getValidInput(
                    "value", input,
                    "ValueCharacters", MAX_LENGTH * VALUE_FACTOR, false);
        } catch (ValidationException e) {
            final String message = "Validation error: [" + input + "]," + e.getMessage();
            logger.error(message);
            throw new BadRequestException(message);
        }
        return resultValue;
    }

    public static List<Collection> getCollectionSubList(
            List<Collection> list, int offSet, int size) {
        List<Collection> result = Collections.emptyList();
        int length = list.size();
        if (offSet < 0 || size < 0 || length == 0) {
            return result;
        }
        offSet = offSet == 0 ? 1 : offSet;
        list.sort(Comparator.comparing(Collection::getName));
        int start = (offSet - 1) * size;
        if (start >= length) {
            return result;
        }
        if (offSet * size > length) {
            result = list.subList(start, length);
        } else {
            result = list.subList(start, start + size);
        }
        return result;
    }

    public static List<Document> getDocumentSubList(
            List<Document> list, int offSet, int size) {
        List<Document> result = Collections.emptyList();
        int length = list.size();
        if (offSet < 0 || size < 0 || length == 0) {
            return result;
        }
        offSet = offSet == 0 ? 1 : offSet;
        list.sort(Comparator.comparing(Document::getKey));
        int start = (offSet - 1) * size;
        if (start >= length) {
            return result;
        }
        if (offSet * size > length) {
            result = list.subList(start, length);
        } else {
            result = list.subList(start, start + size);
        }
        return result;
    }

    public static boolean isInvalidToJsonSchema(String value, String schema) {
        try {
            InputStream schemaInputStream = new ByteArrayInputStream(
                    schema.getBytes(StandardCharsets.UTF_8));
            InputStream objectInputStream =
                    new ByteArrayInputStream(value.replaceAll("\\s", "")
                            .getBytes(StandardCharsets.UTF_8));
            JSONObject jsonSchema = new JSONObject(
                    new JSONTokener(schemaInputStream));
            JSONObject jsonObject = new JSONObject(
                    new JSONTokener(objectInputStream));
            SchemaLoader.load(jsonSchema).validate(jsonObject);
        } catch (org.everit.json.schema.ValidationException e) {
            System.err.println("Validation exception: " + e.getMessage());
            e.getCausingExceptions().stream()
                    .map(org.everit.json.schema.ValidationException::getMessage)
                    .forEach(System.out::println);
            throw new BadRequestException(
                    "Validation error: [" + value + "]," + e.getMessage());
        }
        return false;
    }

}