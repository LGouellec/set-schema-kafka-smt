package com.github.lgouellec.kafka.connect.smt.internal;

import com.github.lgouellec.kafka.connect.smt.utils.Utils;
import io.confluent.kafka.schemaregistry.SchemaProvider;
import io.confluent.kafka.schemaregistry.client.MockSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import io.confluent.kafka.schemaregistry.json.JsonSchema;
import io.confluent.kafka.schemaregistry.json.JsonSchemaProvider;
import io.confluent.kafka.serializers.subject.TopicNameStrategy;
import org.apache.kafka.connect.data.Schema;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;

public class SchemaCacheTest {

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
            "      \"type\": \"number\"\n" +
            "    },\n" +
            "    \"person\": {\n" +
            "      \"type\": \"object\",\n" +
            "      \"properties\": {\n" +
            "        \"name\": {\n" +
            "          \"type\": \"string\"\n" +
            "        },\n" +
            "        \"age\": {\n" +
            "          \"type\": \"number\"\n" +
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
    public void getSchemaNotPresentInCache() throws NoSuchMethodException {

        SchemaCache cache = new SchemaCache(
                mockSchemaRegistryClient,
                TopicNameStrategy.class,
                100,
                50,
                "JSON");

        Schema schema = cache.getSchema(Utils.buildSourceRecord(topic));
        assertNotNull(schema);
    }

    @Test
    public void getSchemaAlreadyPresentInCache() throws NoSuchMethodException {

        SchemaCache cache = new SchemaCache(
                mockSchemaRegistryClient,
                TopicNameStrategy.class,
                100,
                50,
                "JSON");

        Schema schema = cache.getSchema(Utils.buildSourceRecord(topic));
        assertNotNull(schema);

        Schema schemabis = cache.getSchema(Utils.buildSourceRecord(topic));
        assertNotNull(schemabis);
    }

    @Test
    public void getSchemaIncorrect() throws NoSuchMethodException {

        SchemaCache cache = new SchemaCache(
                mockSchemaRegistryClient,
                TopicNameStrategy.class,
                100,
                50,
                "JSON");

        Schema schema = cache.getSchema(Utils.buildSourceRecord("unknow"));
        assertNull(schema);
    }
}
