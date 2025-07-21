package com.github.lgouellec.kafka.connect.smt;

import com.github.lgouellec.kafka.connect.smt.converter.JsonSchemaData2;
import com.github.lgouellec.kafka.connect.smt.converter.JsonSchemaDataConfig2;
import io.confluent.connect.json.JsonSchemaData;
import io.confluent.connect.json.JsonSchemaDataConfig;
import io.confluent.kafka.schemaregistry.json.JsonSchema;
import org.everit.json.schema.ObjectSchema;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Map;

public class JsonSchemaDataReproduceTest {
    public final String schema = "{\n" +
            "  \"$schema\": \"http://json-schema.org/draft-07/schema#\",\n" +
            "  \"additionalProperties\": false,\n" +
            "  \"definitions\": {\n" +
            "    \"Address\": {\n" +
            "      \"additionalProperties\": false,\n" +
            "      \"properties\": {\n" +
            "        \"doornumber\": {\n" +
            "          \"type\": \"integer\"\n" +
            "        },\n" +
            "        \"doorpin\": {\n" +
            "          \"confluent:tags\": [\n" +
            "            \"test_jd_encrypt_global_1\"\n" +
            "          ],\n" +
            "          \"type\": \"string\"\n" +
            "        },\n" +
            "        \"state\": {\n" +
            "          \"type\": \"string\"\n" +
            "        },\n" +
            "        \"street\": {\n" +
            "          \"type\": \"string\"\n" +
            "        }\n" +
            "      },\n" +
            "      \"required\": [\n" +
            "        \"doornumber\",\n" +
            "        \"street\",\n" +
            "        \"doorpin\"\n" +
            "      ],\n" +
            "      \"type\": \"object\"\n" +
            "    }\n" +
            "  },\n" +
            "  \"properties\": {\n" +
            "    \"address\": {\n" +
            "      \"$ref\": \"#/definitions/Address\"\n" +
            "    },\n" +
            "    \"firstname\": {\n" +
            "      \"type\": \"string\"\n" +
            "    },\n" +
            "    \"lastname\": {\n" +
            "      \"type\": \"string\"\n" +
            "    },\n" +
            "    \"nas\": {\n" +
            "      \"confluent:tags\": [\n" +
            "        \"test_jd_encrypt_global_1\"\n" +
            "      ],\n" +
            "      \"type\": \"string\"\n" +
            "    }\n" +
            "  },\n" +
            "  \"required\": [\n" +
            "    \"firstname\",\n" +
            "    \"lastname\",\n" +
            "    \"nas\",\n" +
            "    \"address\"\n" +
            "  ],\n" +
            "  \"title\": \"Sample Event\",\n" +
            "  \"type\": \"object\"\n" +
            "}";

    private JsonSchemaData jsonSchemaData;
    private JsonSchemaData2 jsonSchemaData2;

    @BeforeEach
    public void init() {
        Map<String, ?> config = Map.of(
                JsonSchemaDataConfig.OBJECT_ADDITIONAL_PROPERTIES_CONFIG, false,
                JsonSchemaDataConfig.USE_OPTIONAL_FOR_NON_REQUIRED_CONFIG, true);
        jsonSchemaData = new JsonSchemaData(new JsonSchemaDataConfig(config));

        Map<String, ?> config2 = Map.of(
                JsonSchemaDataConfig2.OBJECT_ADDITIONAL_PROPERTIES_CONFIG, false,
                JsonSchemaDataConfig2.ENFORCE_REQUIRED_FIELDS_CONFIG, true);

        jsonSchemaData2 = new JsonSchemaData2(new JsonSchemaDataConfig2(config2));
    }

    @Test
    public void tryConvertFromAndTo() {

        // SMT
        var connectSchema = jsonSchemaData.toConnectSchema(new JsonSchema(schema));
        // S3 payload JSON brut
        // Create a Struct => Object schema
        // END SMT

        // CONVERTER
        var jsonNodeSchema = jsonSchemaData.fromConnectSchema(connectSchema);
        // jsonNodeSchema = le schema registry is compatible

        Assertions.assertEquals(4, ((ObjectSchema)jsonNodeSchema.rawSchema()).getRequiredProperties().size());
    }

    @Test
    public void tryConvertFromAndTo2() {
        var connectSchema = jsonSchemaData2.toConnectSchema(new JsonSchema(schema));
        var jsonNodeSchema = jsonSchemaData2.fromConnectSchema(connectSchema);
        Assertions.assertEquals(4, ((ObjectSchema)jsonNodeSchema.rawSchema()).getRequiredProperties().size());
    }
}
