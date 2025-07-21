package com.github.lgouellec.kafka.connect.smt;

import com.github.lgouellec.kafka.connect.smt.converter.JsonSchemaConverter2;
import com.github.lgouellec.kafka.connect.smt.converter.JsonSchemaData2;
import com.github.lgouellec.kafka.connect.smt.converter.JsonSchemaDataConfig2;
import io.confluent.connect.json.JsonSchemaConverter;
import io.confluent.connect.json.JsonSchemaData;
import io.confluent.connect.json.JsonSchemaDataConfig;
import io.confluent.kafka.schemaregistry.SchemaProvider;
import io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import io.confluent.kafka.schemaregistry.json.JsonSchema;
import io.confluent.kafka.schemaregistry.json.JsonSchemaProvider;
import io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig;
import org.apache.kafka.connect.source.SourceRecord;
import org.everit.json.schema.ObjectSchema;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class Main {
    public static void main(String[] args) {

        HashMap<String, Object> data = new HashMap<>();
        data.put("firstname", "Sylvain");
        data.put("lastname", "Le Gouellec");
        data.put("nas", "-");
        data.put("address",
                Map.of("doorpin", "1234","state", "QC","street", "street","doornumber", 4321));


        Map<String, String> mapConfig = new HashMap<>();
        mapConfig.put("schema.registry." + AbstractKafkaSchemaSerDeConfig.AUTO_REGISTER_SCHEMAS, "false");
        mapConfig.put("schema.registry." + AbstractKafkaSchemaSerDeConfig.BASIC_AUTH_CREDENTIALS_SOURCE, "USER_INFO");
        mapConfig.put("schema.registry." + AbstractKafkaSchemaSerDeConfig.USER_INFO_CONFIG, "XXXX:YYYY");
        mapConfig.put("schema.registry." + AbstractKafkaSchemaSerDeConfig.USE_LATEST_VERSION, "true");

        mapConfig.put(SetSchema.ConfigName.SCHEMA_REGISTRY_URL, "https://CCCCCC.confluent.cloud");
        mapConfig.put(SetSchema.ConfigName.SUBJECT_NAME_STRATEGY, "io.confluent.kafka.serializers.subject.TopicNameStrategy");
        mapConfig.put(SetSchema.ConfigName.SCHEMA_CACHE_TTL_MS, "600000");
        mapConfig.put(SetSchema.ConfigName.SCHEMA_CACHE_MAXIMUM_SIZE, "50");
        mapConfig.put(JsonSchemaDataConfig2.OBJECT_ADDITIONAL_PROPERTIES_CONFIG, "false");
        mapConfig.put(JsonSchemaDataConfig2.ENFORCE_REQUIRED_FIELDS_CONFIG, "true");

        SetSchema<SourceRecord> setSchemaSMT = new SetSchema<>();
        setSchemaSMT.configure(mapConfig);

        SourceRecord sourceRecord = new SourceRecord(
                new HashMap<String, String>(),
                new HashMap<String, String>(),
                "address",
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
        schemaRegistryClientConfig.put(JsonSchemaDataConfig2.OBJECT_ADDITIONAL_PROPERTIES_CONFIG, "false");
        schemaRegistryClientConfig.put(JsonSchemaDataConfig2.ENFORCE_REQUIRED_FIELDS_CONFIG, "true");

        var schemaRegistryClient = new CachedSchemaRegistryClient(
                (String)mapConfig.get(SetSchema.ConfigName.SCHEMA_REGISTRY_URL),
                100,
                providers,
                schemaRegistryClientConfig);

        var jsonSchemaConverter = new JsonSchemaConverter2(schemaRegistryClient);
        jsonSchemaConverter.configure(schemaRegistryClientConfig, false);

        try {
            byte[] results = jsonSchemaConverter.fromConnectData(newRecord.topic(), null, newRecord.valueSchema(), newRecord.value());
            System.out.println(results);
        }finally {
            setSchemaSMT.close();
        }
    }
}