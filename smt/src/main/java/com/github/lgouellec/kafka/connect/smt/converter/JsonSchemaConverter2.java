package com.github.lgouellec.kafka.connect.smt.converter;

import com.fasterxml.jackson.databind.JsonNode;
import com.google.common.annotations.VisibleForTesting;
import io.confluent.connect.json.JsonSchemaConverterConfig;
import io.confluent.connect.json.JsonSchemaDataConfig;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClientFactory;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.common.errors.InvalidConfigurationException;
import org.apache.kafka.common.errors.NetworkException;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.errors.TimeoutException;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.errors.DataException;
import org.apache.kafka.connect.errors.RetriableException;
import org.apache.kafka.connect.storage.Converter;

import java.util.Collections;
import java.util.Map;

import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.schemaregistry.json.JsonSchema;
import io.confluent.kafka.schemaregistry.json.JsonSchemaProvider;
import io.confluent.kafka.serializers.AbstractKafkaSchemaSerDe;
import io.confluent.kafka.serializers.json.AbstractKafkaJsonSchemaDeserializer;
import io.confluent.kafka.serializers.json.AbstractKafkaJsonSchemaSerializer;
import io.confluent.kafka.serializers.json.JsonSchemaAndValue;
import io.confluent.kafka.serializers.json.KafkaJsonSchemaDeserializerConfig;
import io.confluent.kafka.serializers.json.KafkaJsonSchemaSerializerConfig;

public class JsonSchemaConverter2 extends AbstractKafkaSchemaSerDe implements Converter {

    private SchemaRegistryClient schemaRegistry;
    private Serializer serializer;
    private Deserializer deserializer;

    private boolean isKey;
    private JsonSchemaData2 jsonSchemaData;

    public JsonSchemaConverter2() {
    }

    @VisibleForTesting
    public JsonSchemaConverter2(SchemaRegistryClient client) {
        schemaRegistry = client;
    }

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {
        this.isKey = isKey;
        JsonSchemaConverterConfig jsonSchemaConverterConfig = new JsonSchemaConverterConfig(configs);

        if (schemaRegistry == null) {
            schemaRegistry = SchemaRegistryClientFactory.newClient(
                    jsonSchemaConverterConfig.getSchemaRegistryUrls(),
                    jsonSchemaConverterConfig.getMaxSchemasPerSubject(),
                    Collections.singletonList(new JsonSchemaProvider()),
                    configs,
                    jsonSchemaConverterConfig.requestHeaders()
            );
        }

        serializer = new Serializer(configs, schemaRegistry);
        deserializer = new Deserializer(configs, schemaRegistry);
        jsonSchemaData = new JsonSchemaData2(new JsonSchemaDataConfig2(configs));
    }

    @Override
    public byte[] fromConnectData(String topic, Schema schema, Object value) {
        return fromConnectData(topic, null, schema, value);
    }

    @Override
    public byte[] fromConnectData(String topic, Headers headers, Schema schema, Object value) {
        if (schema == null && value == null) {
            return null;
        }
        JsonSchema jsonSchema = jsonSchemaData.fromConnectSchema(schema);
        JsonNode jsonValue = jsonSchemaData.fromConnectData(schema, value);
        try {
            return serializer.serialize(topic, headers, isKey, jsonValue, jsonSchema);
        } catch (TimeoutException e) {
            throw new RetriableException(String.format("Converting Kafka Connect data to byte[] failed "
                            + "due to serialization error of topic %s: ",
                    topic),
                    e
            );
        } catch (SerializationException e) {
            if (e.getCause() instanceof java.io.IOException) {
                throw new NetworkException(
                        String.format("I/O error while serializing Json data for topic %s: %s",
                                topic, e.getCause().getMessage()),
                        e
                );
            } else {
                throw new DataException(
                        String.format("Converting Kafka Connect data to byte[] failed due to "
                                        + "serialization error of topic %s: ",
                                topic),
                        e
                );
            }
        } catch (InvalidConfigurationException e) {
            throw new ConfigException(
                    String.format("Failed to access JSON Schema data from "
                            + "topic %s : %s", topic, e.getMessage())
            );
        }
    }

    @Override
    public SchemaAndValue toConnectData(String topic, byte[] value) {
        return toConnectData(topic, null, value);
    }

    @Override
    public SchemaAndValue toConnectData(String topic, Headers headers, byte[] value) {
        try {
            JsonSchemaAndValue deserialized = deserializer.deserialize(topic, isKey, headers, value);

            if (deserialized == null || deserialized.getValue() == null) {
                return SchemaAndValue.NULL;
            }

            JsonSchema jsonSchema = deserialized.getSchema();
            Schema schema = jsonSchemaData.toConnectSchema(jsonSchema);
            return new SchemaAndValue(schema, jsonSchemaData.toConnectData(schema,
                    (JsonNode) deserialized.getValue()));
        } catch (TimeoutException e) {
            throw new RetriableException(String.format("Converting byte[] to Kafka Connect data failed "
                            + "due to serialization error of topic %s: ",
                    topic),
                    e
            );
        } catch (SerializationException e) {
            if (e.getCause() instanceof java.io.IOException) {
                throw new NetworkException(
                        String.format("I/O error while deserializing data for topic %s: %s",
                                topic, e.getCause().getMessage()),
                        e
                );
            } else {
                throw new DataException(
                        String.format("Converting byte[] to Kafka Connect data failed due to "
                                        + "serialization error of topic %s: ",
                                topic),
                        e
                );
            }
        } catch (InvalidConfigurationException e) {
            throw new ConfigException(
                    String.format("Failed to access JSON Schema data from "
                            + "topic %s : %s", topic, e.getMessage())
            );
        }
    }

    static class Serializer extends AbstractKafkaJsonSchemaSerializer {

        public Serializer(SchemaRegistryClient client, boolean autoRegisterSchema) {
            schemaRegistry = client;
            this.autoRegisterSchema = autoRegisterSchema;
        }

        public Serializer(Map<String, ?> configs, SchemaRegistryClient client) {

            this(client, false);
            configure(new KafkaJsonSchemaSerializerConfig(configs));
        }

        public byte[] serialize(
                String topic, Headers headers, boolean isKey, Object value, JsonSchema schema) {
            if (value == null) {
                return null;
            }
            return serializeImpl(
                    getSubjectName(topic, isKey, value, schema), topic, headers, value, schema);
        }
    }

    static class Deserializer extends AbstractKafkaJsonSchemaDeserializer {

        public Deserializer(SchemaRegistryClient client) {
            schemaRegistry = client;
        }

        public Deserializer(Map<String, ?> configs, SchemaRegistryClient client) {
            this(client);
            configure(new KafkaJsonSchemaDeserializerConfig(configs), null);
        }

        public JsonSchemaAndValue deserialize(
                String topic, boolean isKey, Headers headers, byte[] payload) {
            return deserializeWithSchemaAndVersion(topic, isKey, headers, payload);
        }
    }
}