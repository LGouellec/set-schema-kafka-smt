package com.github.lgouellec.kafka.connect.smt;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.source.SourceRecord;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;
import com.github.lgouellec.kafka.connect.smt.utils.Utils;

import static org.junit.jupiter.api.Assertions.*;

public class SetRecordNameTest {

    public class Person {
        public String country;
        public String state;
        public String street;
        public int doornumber;
    }

    @Test
    public void setRecordNameHashMapRecord(){

        HashMap<String, Object> data = new HashMap<>();
        data.put("country", "Canada");
        data.put("doornumber", 301);
        data.put("state", "BC");
        data.put("street", "126 21st Street East");

        var record = Utils.buildSourceRecord("person", data);

        SetRecordName<SourceRecord> setRecordName = new SetRecordName.Value<>();
        setRecordName.configure(Map.of(SetRecordName.ConfigName.RECORD_NAME, "person"));

        var newRecord = setRecordName.apply(record);

        assertNotNull(newRecord);
        assertNotNull(newRecord.valueSchema());
        assertEquals("person", newRecord.valueSchema().name());
    }

    @Test
    public void setRecordNameJavaObjectRecord(){

        var data = new Person();
        data.country = "Canada";
        data.doornumber = 301;
        data.state = "BC";
        data.street = "126 21st Street East";

        var record = Utils.buildSourceRecord("person", data);

        SetRecordName<SourceRecord> setRecordName = new SetRecordName.Value<>();
        setRecordName.configure(Map.of(SetRecordName.ConfigName.RECORD_NAME, "person"));

        var newRecord = setRecordName.apply(record);

        assertNotNull(newRecord);
        assertNotNull(newRecord.valueSchema());
        assertEquals("person", newRecord.valueSchema().name());
    }

    @Test
    public void setRecordNameNullValueRecord(){

        var record = Utils.buildSourceRecord("person", null);

        SetRecordName<SourceRecord> setRecordName = new SetRecordName.Value<>();
        setRecordName.configure(Map.of(SetRecordName.ConfigName.RECORD_NAME, "person"));

        var newRecord = setRecordName.apply(record);

        assertNotNull(newRecord);
        assertNull(newRecord.valueSchema());
    }

    @Test
    public void setRecordNameWithExistingSchema(){

        var data = new Person();
        data.country = "Canada";
        data.doornumber = 301;
        data.state = "BC";
        data.street = "126 21st Street East";

        Schema valueSchema =
                new SchemaBuilder(Schema.Type.STRUCT)
                        .field("country", Schema.STRING_SCHEMA)
                        .field("doornumber", Schema.INT32_SCHEMA)
                        .field("state", Schema.STRING_SCHEMA)
                        .field("street", Schema.STRING_SCHEMA)
                        .build();

        var record = Utils.buildSourceRecord("person", valueSchema, data);

        SetRecordName<SourceRecord> setRecordName = new SetRecordName.Value<>();
        setRecordName.configure(Map.of(SetRecordName.ConfigName.RECORD_NAME, "person"));

        var newRecord = setRecordName.apply(record);

        assertNotNull(newRecord);
        assertNotNull(newRecord.valueSchema());
        assertEquals(4L, newRecord.valueSchema().fields().size());
        assertEquals("person", newRecord.valueSchema().name());
    }
}