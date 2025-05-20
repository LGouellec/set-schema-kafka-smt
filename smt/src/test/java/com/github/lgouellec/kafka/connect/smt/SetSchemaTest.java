package com.github.lgouellec.kafka.connect.smt;

import com.fasterxml.jackson.core.json.JsonReadFeature;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.MappingIterator;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.lgouellec.kafka.connect.smt.utils.Utils;
import io.confluent.kafka.schemaregistry.SchemaProvider;
import io.confluent.kafka.schemaregistry.client.MockSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import io.confluent.kafka.schemaregistry.json.JsonSchema;
import io.confluent.kafka.schemaregistry.json.JsonSchemaProvider;
import io.confluent.kafka.serializers.subject.TopicNameStrategy;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

public class SetSchemaTest {
    public final String topic = "person";
    public final String schema = "{\n" +
            "  \"$schema\": \"http://json-schema.org/draft-07/schema#\",\n" +
            "  \"title\": \"Generated schema for Root\",\n" +
            "  \"type\": \"object\",\n" +
            "  \"properties\": {\n" +
            "    \"country\": {\n" +
            "      \"type\": \"string\"\n" +
            "    },\n" +
            "    \"state\": {\n" +
            "      \"type\": \"string\"\n" +
            "    },\n" +
            "    \"street\": {\n" +
            "      \"type\": \"string\"\n" +
            "    },\n" +
            "    \"doornumber\": {\n" +
            "      \"type\": \"number\",\n" +
            "      \"connect.type\" : \"int32\"\n" +
            "    },\n" +
            "    \"person\": {\n" +
            "      \"type\": \"object\",\n" +
            "      \"properties\": {\n" +
            "        \"name\": {\n" +
            "          \"type\": \"string\"\n" +
            "        },\n" +
            "        \"age\": {\n" +
            "          \"type\": \"number\",\n" +
            "          \"connect.type\" : \"int32\"\n" +
            "        }\n" +
            "      },\n" +
            "      \"required\": [\n" +
            "        \"name\",\n" +
            "        \"age\"\n" +
            "      ]\n" +
            "    },\n" +
            "    \"cars\": {\n" +
            "      \"type\": \"array\",\n" +
            "      \"items\": {\n" +
            "        \"type\": \"object\",\n" +
            "        \"properties\": {\n" +
            "          \"name\": {\n" +
            "            \"type\": \"string\"\n" +
            "          },\n" +
            "          \"model\": {\n" +
            "            \"type\": \"string\"\n" +
            "          }\n" +
            "        },\n" +
            "        \"required\": [\n" +
            "          \"name\",\n" +
            "          \"model\"\n" +
            "        ]\n" +
            "      }\n" +
            "    }\n" +
            "  },\n" +
            "  \"required\": [\n" +
            "    \"country\",\n" +
            "    \"state\",\n" +
            "    \"street\",\n" +
            "    \"doornumber\",\n" +
            "    \"person\",\n" +
            "    \"cars\"\n" +
            "  ]\n" +
            "}" ;

    private MockSchemaRegistryClient mockSchemaRegistryClient;

    @BeforeEach
    public void init() throws RestClientException, IOException {
        List<SchemaProvider> providers = List.of(new JsonSchemaProvider());
        mockSchemaRegistryClient = new MockSchemaRegistryClient(providers);
        mockSchemaRegistryClient.register(topic + "-value", new JsonSchema(schema));
    }

    @AfterEach
    public void dispose(){
        mockSchemaRegistryClient.reset();
    }

    @Test
    public void setJsonSchemaHashMapRecord(){

        HashMap<String, Object> data = new HashMap<>();
        data.put("country", "Canada");
        data.put("doornumber", 301);
        data.put("state", "BC");
        data.put("street", "126 21st Street East");
        data.put("person", Map.of("age", 31, "name", "Pedro"));
        data.put("cars", List.of(Map.of("name", "Kia", "model", "Sorento")));

        SetSchema<SourceRecord> setJsonSchema = new SetSchema<>();
        setJsonSchema.configure(
                Map.of(
                        SetSchema.ConfigName.SCHEMA_CACHE_TTL_MS, 10 * 60 * 1000,
                        SetSchema.ConfigName.SCHEMA_CACHE_MAXIMUM_SIZE, 50,
                        SetSchema.ConfigName.SUBJECT_NAME_STRATEGY, TopicNameStrategy.class,
                        SetSchema.ConfigName.SCHEMA_REGISTRY_URL, "mock://url"));

        setJsonSchema.setSchemaRegistryClient(mockSchemaRegistryClient);

        var record = Utils.buildSourceRecord(topic, data);
        var newRecord = setJsonSchema.apply(record);
        assertNotNull(newRecord.valueSchema());
        assertInstanceOf(Struct.class, newRecord.value());
        assertEquals("Canada", ((Struct)newRecord.value()).get("country"));
        assertEquals(301, ((Struct)newRecord.value()).get("doornumber"));
        assertEquals("BC", ((Struct)newRecord.value()).get("state"));
        assertEquals("126 21st Street East", ((Struct)newRecord.value()).get("street"));
        assertInstanceOf(Struct.class,  ((Struct)newRecord.value()).get("person"));
        assertEquals(31,  ((Struct)((Struct)newRecord.value()).get("person")).get("age"));
        assertEquals("Pedro",  ((Struct)((Struct)newRecord.value()).get("person")).get("name"));
        assertInstanceOf(List.class,  ((Struct)newRecord.value()).get("cars"));
        assertInstanceOf(Struct.class,  ((List)((Struct)newRecord.value()).get("cars")).get(0));
        assertEquals(1,  ((List)((Struct)newRecord.value()).get("cars")).size());
        assertEquals("Kia",  ((Struct)((List)((Struct)newRecord.value()).get("cars")).get(0)).get("name"));
        assertEquals("Sorento",  ((Struct)((List)((Struct)newRecord.value()).get("cars")).get(0)).get("model"));
    }

    @Test
    public void setJsonSchemaJsonNodeRecord() throws IOException {

        String jsonPayload = "{\n" +
                "    \"country\": \"Poland\",\n" +
                "    \"state\": \"-\",\n" +
                "    \"street\": \"Varsovie\",\n" +
                "    \"doornumber\": 343,\n" +
                "    \"person\": {\n" +
                "        \"name\": \"sylvain\",\n" +
                "        \"age\": 31\n" +
                "    },\n" +
                "    \"cars\": [\n" +
                "        {\n" +
                "            \"name\": \"Kia\",\n" +
                "            \"model\": \"Sorento\"\n" +
                "        },\n" +
                "        {\n" +
                "            \"name\": \"Seat\",\n" +
                "            \"model\": \"Ateca\"\n" +
                "        }\n" +
                "    ]\n" +
                "}";

        InputStream stringStream = new ByteArrayInputStream(jsonPayload.getBytes());
        Map<String, Object> converterConfig = new HashMap<>();
        converterConfig.put("schemas.enable", false);
        converterConfig.put("schemas.cache.size", 100);

        var mapper = new ObjectMapper();
        MappingIterator<JsonNode> reader = mapper.readerFor(JsonNode.class).with(JsonReadFeature.ALLOW_NON_NUMERIC_NUMBERS)
                .readValues(stringStream);
        JsonNode nodeRecord = reader.next();


        SetSchema<SourceRecord> setJsonSchema = new SetSchema<>();
        setJsonSchema.configure(
                Map.of(
                        SetSchema.ConfigName.SCHEMA_CACHE_TTL_MS, 10 * 60 * 1000,
                        SetSchema.ConfigName.SCHEMA_CACHE_MAXIMUM_SIZE, 50,
                        SetSchema.ConfigName.SUBJECT_NAME_STRATEGY, TopicNameStrategy.class,
                        SetSchema.ConfigName.SCHEMA_REGISTRY_URL, "mock://url"));

        setJsonSchema.setSchemaRegistryClient(mockSchemaRegistryClient);

        var record = Utils.buildSourceRecord(topic, nodeRecord);
        var newRecord = setJsonSchema.apply(record);

        assertNotNull(newRecord.valueSchema());
        assertInstanceOf(Struct.class, newRecord.value());
        assertEquals("Poland", ((Struct)newRecord.value()).get("country"));
        assertEquals(343, ((Struct)newRecord.value()).get("doornumber"));
        assertEquals("-", ((Struct)newRecord.value()).get("state"));
        assertEquals("Varsovie", ((Struct)newRecord.value()).get("street"));
        assertInstanceOf(Struct.class,  ((Struct)newRecord.value()).get("person"));
        assertEquals(31,  ((Struct)((Struct)newRecord.value()).get("person")).get("age"));
        assertEquals("sylvain",  ((Struct)((Struct)newRecord.value()).get("person")).get("name"));
        assertInstanceOf(List.class,  ((Struct)newRecord.value()).get("cars"));
        assertEquals(2,  ((List)((Struct)newRecord.value()).get("cars")).size());
        assertInstanceOf(Struct.class,  ((List)((Struct)newRecord.value()).get("cars")).get(0));
        assertInstanceOf(Struct.class,  ((List)((Struct)newRecord.value()).get("cars")).get(1));
        assertEquals("Kia",  ((Struct)((List)((Struct)newRecord.value()).get("cars")).get(0)).get("name"));
        assertEquals("Sorento",  ((Struct)((List)((Struct)newRecord.value()).get("cars")).get(0)).get("model"));
        assertEquals("Seat",  ((Struct)((List)((Struct)newRecord.value()).get("cars")).get(1)).get("name"));
        assertEquals("Ateca",  ((Struct)((List)((Struct)newRecord.value()).get("cars")).get(1)).get("model"));
    }
}