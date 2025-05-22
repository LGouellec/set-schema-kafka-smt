package com.github.lgouellec.kafka.connect.smt;

import io.confluent.connect.json.JsonSchemaConverter;
import io.confluent.connect.json.JsonSchemaData;
import io.confluent.kafka.schemaregistry.SchemaProvider;
import io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import io.confluent.kafka.schemaregistry.json.JsonSchema;
import io.confluent.kafka.schemaregistry.json.JsonSchemaProvider;
import io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig;
import org.apache.kafka.connect.source.SourceRecord;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class Main {
    public static void main(String[] args) throws RestClientException, IOException {

        HashMap<String, Object> data = new HashMap<>();
        data.put("country", "Poland");
        data.put("doornumber", 343);
        data.put("state", "-");
        data.put("street", "Varsovie");
        data.put("person", Map.of("name", "sylvain", "age", 31));
        data.put("cars", List.of(
                Map.of("name", "Kia", "model", "Sorento"),
                Map.of("name", "Seat", "model", "Ateca")));


        Map<String, String> mapConfig = new HashMap<>();
        mapConfig.put("schema.registry." + AbstractKafkaSchemaSerDeConfig.AUTO_REGISTER_SCHEMAS, "false");
        mapConfig.put("schema.registry." + AbstractKafkaSchemaSerDeConfig.BASIC_AUTH_CREDENTIALS_SOURCE, "USER_INFO");
        mapConfig.put("schema.registry." + AbstractKafkaSchemaSerDeConfig.USER_INFO_CONFIG, "XXXX:YYY");
        mapConfig.put("schema.registry." + AbstractKafkaSchemaSerDeConfig.USE_LATEST_VERSION, "true");

        mapConfig.put(SetSchema.ConfigName.SCHEMA_REGISTRY_URL, "URL");
        mapConfig.put(SetSchema.ConfigName.SUBJECT_NAME_STRATEGY, "io.confluent.kafka.serializers.subject.TopicNameStrategy");
        mapConfig.put(SetSchema.ConfigName.SCHEMA_CACHE_TTL_MS, "600000");
        mapConfig.put(SetSchema.ConfigName.SCHEMA_CACHE_MAXIMUM_SIZE, "50");

        SetSchema<SourceRecord> setSchemaSMT = new SetSchema<>();
        setSchemaSMT.configure(mapConfig);

        SourceRecord sourceRecord = new SourceRecord(
                new HashMap<String, String>(),
                new HashMap<String, String>(),
                "person",
                0,
                null,
                data
        );

        var newRecord = setSchemaSMT.apply(sourceRecord);

        // try to convert the new record

        List<SchemaProvider> providers = List.of(new JsonSchemaProvider());
        var schemaRegistryClientConfig = new HashMap<String, String>();
        for (String key : mapConfig.keySet()) {
            if (key.startsWith("schema.registry."))
                schemaRegistryClientConfig.put(key.replace("schema.registry.", ""), (String) mapConfig.get(key));
        }

        schemaRegistryClientConfig.put(SetSchema.ConfigName.SCHEMA_REGISTRY_URL, mapConfig.get(SetSchema.ConfigName.SCHEMA_REGISTRY_URL));

        var schemaRegistryClient = new CachedSchemaRegistryClient(
                (String)mapConfig.get(SetSchema.ConfigName.SCHEMA_REGISTRY_URL),
                100,
                providers,
                schemaRegistryClientConfig);

        var jsonSchemaConverter = new JsonSchemaConverter(schemaRegistryClient);
        jsonSchemaConverter.configure(schemaRegistryClientConfig, false);

        byte[] results = jsonSchemaConverter.fromConnectData(newRecord.topic(), null, newRecord.valueSchema(), newRecord.value());
        System.out.println(results);

        setSchemaSMT.close();
    }
}