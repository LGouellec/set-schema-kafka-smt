package com.github.lgouellec.kafka.connect.smt.utils;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.source.SourceRecord;

import java.util.HashMap;

public class Utils {

    public static SourceRecord buildSourceRecord(String topicName){
        return buildSourceRecord(topicName, null, null);
    }

    public static SourceRecord buildSourceRecord(String topicName, Object value){
        return buildSourceRecord(topicName, null, value);
    }

    public static SourceRecord buildSourceRecord(String topicName, Schema valueSchema, Object value){
        return new SourceRecord(
                new HashMap<>(),
                new HashMap<>(),
                topicName,
                0,
                null,
                null,
                valueSchema,
                value);
    }
}
