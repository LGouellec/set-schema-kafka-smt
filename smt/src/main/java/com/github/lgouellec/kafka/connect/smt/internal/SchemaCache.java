package com.github.lgouellec.kafka.connect.smt.internal;

import com.github.lgouellec.kafka.connect.smt.converter.JsonSchemaData2;
import com.github.lgouellec.kafka.connect.smt.converter.JsonSchemaDataConfig2;
import io.confluent.connect.avro.AvroData;
import io.confluent.connect.avro.AvroDataConfig;
import io.confluent.connect.json.JsonSchemaDataConfig;
import io.confluent.connect.protobuf.ProtobufData;
import io.confluent.connect.protobuf.ProtobufDataConfig;
import io.confluent.kafka.schemaregistry.ParsedSchema;
import io.confluent.kafka.schemaregistry.avro.AvroSchema;
import io.confluent.kafka.schemaregistry.client.SchemaMetadata;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import io.confluent.kafka.schemaregistry.json.JsonSchema;
import io.confluent.kafka.schemaregistry.protobuf.ProtobufSchema;
import org.apache.kafka.connect.connector.ConnectRecord;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.errors.DataException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.util.Arrays;
import java.util.Map;

public class SchemaCache {

    private static final Logger LOGGER = LoggerFactory.getLogger(SchemaCache.class);
    private SchemaRegistryClient schemaRegistryClient;
    private final Class<?>  subjectNameStrategy;
    private final JsonSchemaData2 jsonSchemaData;
    private final ProtobufData protobufData;
    private final AvroData avroData;
    private final int cacheTTLms;
    private MicroCache<String, SchemaMetadata> internalCache;
    private final String schemaFormat;

    public void setSchemaRegistryClient(SchemaRegistryClient schemaRegistryClient){
        this.schemaRegistryClient = schemaRegistryClient;
    }

    public SchemaCache(
            SchemaRegistryClient schemaRegistryClient,
            Class<?> subjectNameStrategy,
            int cacheTTLms,
            int cacheMaxSize,
            String schemaFormat,
            Map<String, ?> configs)
    {
        this.schemaRegistryClient = schemaRegistryClient;
        this.subjectNameStrategy = subjectNameStrategy;
        this.cacheTTLms = cacheTTLms;
        this.schemaFormat = schemaFormat;
        this.internalCache = new MicroCache<>(
                key -> LOGGER.info("Schema cache expired :" + key),
                key -> LOGGER.info("Fetching latest schema for subject : " + key),
                (key) -> {
                    try {
                        SchemaMetadata schemaMetadata = this.schemaRegistryClient.getLatestSchemaMetadata(key);
                        if(schemaMetadata != null)
                            internalCache.put(key, schemaMetadata, this.cacheTTLms);
                        return schemaMetadata;
                    } catch (IOException | RuntimeException | RestClientException e) {
                        LOGGER.warn(String.format("Error when fetching schema metadata for subject %s : %s", key, e.getMessage()));
                        return null;
                    }
                },
                cacheMaxSize);

        jsonSchemaData = new JsonSchemaData2(new JsonSchemaDataConfig2(configs));
        protobufData = new ProtobufData(new ProtobufDataConfig(configs));
        avroData = new AvroData(new AvroDataConfig(configs));
    }

    public void close(){
        try {
            internalCache.clear();
            internalCache.close();
            schemaRegistryClient.close();
        } catch (IOException e) {
            LOGGER.warn(String.format("Error when closing the internal schema registry client : %s", e.getMessage()));
        }
    }

    private String getSubjectFromRecord(ConnectRecord<?> record)
            throws NoSuchMethodException, InvocationTargetException, InstantiationException, IllegalAccessException {

        ParsedSchema parsedSchema = switch (schemaFormat.toUpperCase()) {
            case "JSON" -> jsonSchemaData.fromConnectSchema(record.valueSchema());
            case "PROTO" -> protobufData.fromConnectSchema(record.valueSchema());
            case "AVRO" -> {
                var avroSchema = avroData.fromConnectSchema(record.valueSchema());
                yield new AvroSchema(avroSchema);
            }
            default -> null;
        };

        var m = Arrays.stream(subjectNameStrategy.getMethods()).filter(n -> n.getName().equals("subjectName"))
                .findFirst();

        if(m.isPresent()){
            var strategy = subjectNameStrategy.getDeclaredConstructor().newInstance();
            return (String)m.get().invoke(strategy, record.topic(), false, parsedSchema);
        }
        else{
            throw new DataException(String.format("%s does not contain subjectName method. Please use a correct subject name strategy", subjectNameStrategy));
        }
    }

    public Schema getSchema(ConnectRecord<?> record) {
        try {
            String subjectName = getSubjectFromRecord(record);

            var subjectMetadata = internalCache.get(subjectName);

            if(subjectMetadata != null) {
                switch(schemaFormat.toUpperCase()){
                    case "JSON":
                        JsonSchema jsonParsedSchema = new JsonSchema(subjectMetadata.getSchema());
                        return jsonSchemaData.toConnectSchema(jsonParsedSchema);
                    case "PROTO":
                        ProtobufSchema protoParsedSchema = new ProtobufSchema(subjectMetadata.getSchema());
                        return protobufData.toConnectSchema(protoParsedSchema);
                    case "AVRO":
                        AvroSchema avroSchema = new AvroSchema(subjectMetadata.getSchema());
                        return avroData.toConnectSchema(avroSchema.rawSchema());
                }

            }

            return null;
        } catch (NoSuchMethodException | InvocationTargetException | InstantiationException | IllegalAccessException   e) {
            throw new RuntimeException(e);
        }
    }
}
