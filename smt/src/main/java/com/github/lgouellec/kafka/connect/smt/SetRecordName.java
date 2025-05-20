package com.github.lgouellec.kafka.connect.smt;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.confluent.connect.json.JsonSchemaData;
import io.confluent.kafka.schemaregistry.json.JsonSchema;
import io.confluent.kafka.schemaregistry.json.JsonSchemaUtils;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.components.Versioned;
import org.apache.kafka.connect.connector.ConnectRecord;
import org.apache.kafka.connect.data.ConnectSchema;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.transforms.Transformation;
import org.apache.kafka.connect.transforms.util.SimpleConfig;

import java.io.IOException;
import java.util.Map;

public abstract class SetRecordName<R extends ConnectRecord<R>> implements Transformation<R>, Versioned {

    public String recordSchemaName;

    public interface ConfigName {
        String RECORD_NAME = "record.name";
    }

    public static final ConfigDef CONFIG_DEF = new ConfigDef()
            .define(SetRecordName.ConfigName.RECORD_NAME, ConfigDef.Type.STRING, null, ConfigDef.Importance.HIGH, "Specify the record name which must be set in the schema.");


    @Override
    public String version() {
        return "1.0.0";
    }

    @Override
    public R apply(R record) {

        if(record.value() instanceof JsonNode)
        {
            // convert into HashMap
            ObjectMapper mapper = new ObjectMapper();
            Map<String, Object> mapValue = mapper.convertValue(record.value(), new TypeReference<>() {});

            var temporyRecord =  record.newRecord(
                record.topic(),
                record.kafkaPartition(),
                record.keySchema(),
                record.key(),
                record.valueSchema(),
                    mapValue,
                record.timestamp());

            return newRecord(temporyRecord);
        }

        return newRecord(record);
    }

    @Override
    public ConfigDef config() {
        return CONFIG_DEF;
    }

    @Override
    public void close() {

    }

    @Override
    public void configure(Map<String, ?> configs) {
        SimpleConfig config = new SimpleConfig(CONFIG_DEF, configs);
        recordSchemaName = config.getString(ConfigName.RECORD_NAME);
    }

    protected abstract R newRecord(R record);


    public static class Key<R extends ConnectRecord<R>> extends SetRecordName<R> {
        @Override
        protected R newRecord(R record) {
            Schema kSchema;

            if(record.keySchema() == null) {
                JsonSchemaData jsonSchemaData = new JsonSchemaData();
                JsonSchema jsonSchema = null;
                try {
                    jsonSchema = JsonSchemaUtils.getSchema(record.key());
                } catch (IOException e) {
                    // do nothing
                }
                kSchema = jsonSchemaData.toConnectSchema(jsonSchema);
            }
            else
                kSchema = record.keySchema();

            if(kSchema != null) {
                var newSchema = new ConnectSchema(
                        kSchema.type(),
                        kSchema.isOptional(),
                        kSchema.defaultValue(),
                        recordSchemaName,
                        kSchema.version(),
                        kSchema.doc(),
                        kSchema.parameters(),
                        kSchema.fields(),
                        kSchema.keySchema(),
                        kSchema.valueSchema());

                return record.newRecord(
                        record.topic(),
                        record.kafkaPartition(),
                        newSchema,
                        record.key(),
                        record.valueSchema(),
                        record.value(),
                        record.timestamp());
            }

            return record;
        }
    }

    public static class Value<R extends ConnectRecord<R>> extends SetRecordName<R> {
        @Override
        protected R newRecord(R record) {
            Schema vSchema;

            if(record.valueSchema() == null) {
                JsonSchemaData jsonSchemaData = new JsonSchemaData();
                JsonSchema jsonSchema = null;
                try {
                    jsonSchema = JsonSchemaUtils.getSchema(record.value());
                } catch (IOException e) {
                    // do nothing
                }
                vSchema = jsonSchemaData.toConnectSchema(jsonSchema);
            }
            else
                vSchema = record.valueSchema();

            if(vSchema != null) {
                var newSchema = new ConnectSchema(
                        vSchema.type(),
                        vSchema.isOptional(),
                        vSchema.defaultValue(),
                        recordSchemaName,
                        vSchema.version(),
                        vSchema.doc(),
                        vSchema.parameters(),
                        vSchema.fields(),
                        vSchema.type() == Schema.Type.MAP ? vSchema.keySchema() : null,
                        vSchema.type() == Schema.Type.MAP || vSchema.type() == Schema.Type.ARRAY ? vSchema.valueSchema() : null);

                return record.newRecord(
                        record.topic(),
                        record.kafkaPartition(),
                        record.keySchema(),
                        record.key(),
                        newSchema,
                        record.value(),
                        record.timestamp());
            }

            return record;
        }
    }
}
