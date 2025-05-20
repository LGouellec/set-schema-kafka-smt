package com.github.lgouellec.kafka.connect.smt;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.lgouellec.kafka.connect.smt.internal.SchemaCache;
import io.confluent.kafka.schemaregistry.SchemaProvider;
import io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.schemaregistry.json.JsonSchemaProvider;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.components.Versioned;
import org.apache.kafka.connect.connector.ConnectRecord;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.errors.DataException;
import org.apache.kafka.connect.transforms.Transformation;
import org.apache.kafka.connect.transforms.util.SimpleConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class SetSchema<R extends ConnectRecord<R>> implements Transformation<R>, Versioned {

    static {
        try {

            Thread
                    .currentThread()
                    .getContextClassLoader()
                    .loadClass("io.confluent.kafka.serializers.subject.TopicNameStrategy");

            Thread
                    .currentThread()
                    .getContextClassLoader()
                    .loadClass("io.confluent.kafka.serializers.subject.RecordNameStrategy");

            Thread
                    .currentThread()
                    .getContextClassLoader()
                    .loadClass("io.confluent.kafka.serializers.subject.TopicRecordNameStrategy");

        } catch (ClassNotFoundException e) {
            throw new RuntimeException(e);
        }
    }

    private static final Logger log = LoggerFactory.getLogger(SetSchema.class);
    private final Map<String, String> schemaRegistryClientConfig = new HashMap<>();
    private String schemaRegistryUrl;
    private SchemaCache schemaCache;
    private String failStrategy;
    private String schemaFormat;

    public interface ConfigName {
        String SUBJECT_NAME_STRATEGY = "subject.name.strategy";
        String FAIL_STRATEGY = "fail.strategy";
        String SCHEMA_REGISTRY_URL = "schema.registry.url";
        String SCHEMA_CACHE_TTL_MS = "cache.ttl.ms";
        String SCHEMA_CACHE_MAXIMUM_SIZE = "cache.max.size";
        String SCHEMA_FORMAT = "schema.format";
    }

    public static final ConfigDef CONFIG_DEF = new ConfigDef()
            .define(ConfigName.SCHEMA_REGISTRY_URL, ConfigDef.Type.STRING, null, ConfigDef.Importance.HIGH, "Schema registry url to set.")
            .define(ConfigName.SCHEMA_FORMAT, ConfigDef.Type.STRING, "JSON", ConfigDef.Importance.HIGH, "Schema format used (JSON, PROTO or AVRO). Default: JSON")
            .define(ConfigName.FAIL_STRATEGY, ConfigDef.Type.STRING, "FAIL", ConfigDef.Importance.HIGH, "Strategy if the conversion fails (FAIL or CONTINUE). Default: FAIL")
            .define(ConfigName.SUBJECT_NAME_STRATEGY, ConfigDef.Type.CLASS, null, ConfigDef.Importance.HIGH, "Determines how to construct the subject name under which the value schema is registered in the Schema Registry.")
            .define(ConfigName.SCHEMA_CACHE_TTL_MS, ConfigDef.Type.INT, 1000 * 60 * 10, ConfigDef.Importance.MEDIUM, "Configure the TTL expiration in the cache which contains schemas (auto-removes old data).")
            .define(ConfigName.SCHEMA_CACHE_MAXIMUM_SIZE, ConfigDef.Type.INT, 100, ConfigDef.Importance.MEDIUM, "Configure the maximum size of the internal cache of schemas.");

    // visible for testing
    void setSchemaRegistryClient(SchemaRegistryClient client){
        schemaCache.setSchemaRegistryClient(client);
    }

    @Override
    public void configure(Map<String, ?> configs) {
        final SimpleConfig config = new SimpleConfig(CONFIG_DEF, configs);

        for (String key : configs.keySet()) {
            if (key.startsWith("schema.registry."))
                schemaRegistryClientConfig.put(key.replace("schema.registry.", ""), (String) configs.get(key));
        }

        schemaRegistryUrl = config.getString(ConfigName.SCHEMA_REGISTRY_URL);
        failStrategy = config.getString(ConfigName.FAIL_STRATEGY);
        schemaFormat = config.getString(ConfigName.SCHEMA_FORMAT);

        var subjectNameStrategy = config.getClass(ConfigName.SUBJECT_NAME_STRATEGY);
        int ttlCache = config.getInt(ConfigName.SCHEMA_CACHE_TTL_MS);
        int maxCacheSize = config.getInt(ConfigName.SCHEMA_CACHE_MAXIMUM_SIZE);

        schemaRegistryClientConfig.remove(ConfigName.SCHEMA_REGISTRY_URL);

        List<SchemaProvider> providers = List.of(new JsonSchemaProvider());
        var schemaRegistryClient = new CachedSchemaRegistryClient(
                schemaRegistryUrl,
                100,
                providers,
                schemaRegistryClientConfig);

        schemaCache = new SchemaCache(
                schemaRegistryClient,
                subjectNameStrategy,
                ttlCache,
                maxCacheSize,
                schemaFormat);
    }

    @Override
    public String version() {
        return "1.0.0";
    }

    @Override
    public R apply(R r) {
        try {
            Object newObj = null;
            var schema = schemaCache.getSchema(r);

            if (schema != null) {
                if (r.value() instanceof Map<?, ?>) {
                    Map<String, Object> value = (Map<String, Object>) r.value();
                    newObj = mapToStructObj(schema, value);
                }
                else if (r.value() instanceof JsonNode)
                    newObj = jsonNodeToStructObj(schema, (JsonNode) r.value());
                else
                    throw new DataException("Cannot convert the record value type " + r.value().getClass());
            }

            if (newObj != null)
                return r.newRecord(
                        r.topic(),
                        r.kafkaPartition(),
                        r.keySchema(),
                        r.key(),
                        schema,
                        newObj,
                        r.timestamp());

            return r;

        }catch(Exception e){

            log.error(String.format("An exception has been raised during the conversion process. fail.strategy will be executed %s", failStrategy), e);

            if(failStrategy.toUpperCase().equals("FAIL"))
                throw e;
            if(failStrategy.toUpperCase().equals("CONTINUE"))
                return r;

            // default FAIL
            throw e;
        }
    }

    public Object mapToStructObj(Schema schema, Map<String, Object> value)
    {

        Struct obj = new Struct(schema);

        for(Field field : schema.fields()){

            if(value.containsKey(field.name())){
                Object v = null;

                switch(field.schema().type()){
                    case BOOLEAN -> v = Boolean.valueOf(value.get(field.name()).toString());
                    case INT8 -> v = Byte.parseByte(value.get(field.name()).toString());
                    case INT16 -> v = Short.parseShort(value.get(field.name()).toString());
                    case INT32 -> v = Integer.parseInt(value.get(field.name()).toString());
                    case INT64 -> v = Long.parseLong(value.get(field.name()).toString());
                    case STRING -> v = value.get(field.name()).toString();
                    case FLOAT32 -> v = Float.parseFloat(value.get(field.name()).toString());
                    case FLOAT64 -> v = Double.parseDouble(value.get(field.name()).toString());
                    case BYTES -> v = value.get(field.name());
                    case MAP, STRUCT -> {
                        Object o = value.get(field.name());
                        if(o instanceof Map<?, ?>)
                            v = mapToStructObj(field.schema(), (Map<String, Object>)o);
                        else if(o instanceof JsonNode)
                            v = jsonNodeToStructObj(field.schema(), (JsonNode)o);
                        else
                            throw new DataException(String.format("Invalid type for field %s. Type attended is Map<String, Object> but was %s", field.name(), o.getClass()));
                    }
                    case ARRAY -> {
                        Object o = value.get(field.name());
                        ArrayList<Object> newValueList = new ArrayList<>();

                        if(o instanceof List)
                        {
                            for(Object inner : (List)o){
                                if(inner instanceof Map<?, ?>)
                                    newValueList.add(mapToStructObj(field.schema().valueSchema(), (Map<String, Object>)inner));
                                else
                                    throw new DataException(String.format("Invalid inner type for the array field %s[]. Type attended is Map<String, Object> but was %s", field.name(), inner.getClass()));
                            }
                            v = newValueList;
                        }
                        else
                            throw new DataException(String.format("Invalid type for field %s. Type attended is List<?> but was %s", field.name(), o.getClass()));

                    }
                    // STRUCT not supported yet
                }

                obj.put(field.name(), v);
            }
            else {
                if(!field.schema().isOptional())
                    throw new DataException(
                            String.format("Invalid record value. Field %s from schema is required whereas it's not present in the payload", field.name()));
                else
                    log.debug(String.format("Field optional %s is not present in the payload", field.name()));
            }
        }

        return obj;
    }

    public Object jsonNodeToStructObj(Schema schema, JsonNode value){
        ObjectMapper mapper = new ObjectMapper();
        Map<String, Object> mapValue = mapper.convertValue(value, new TypeReference<>() {});
        return mapToStructObj(schema, mapValue);
    }

    @Override
    public ConfigDef config() {
        return CONFIG_DEF;
    }

    @Override
    public void close() {
       schemaCache.close();
    }
}
