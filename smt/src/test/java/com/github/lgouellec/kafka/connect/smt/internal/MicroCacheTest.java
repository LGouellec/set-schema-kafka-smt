package com.github.lgouellec.kafka.connect.smt.internal;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.assertEquals;

public class MicroCacheTest {

    private MicroCache<String, String> cache;
    private int defaultTTLMs = 1000 * 60 * 10; // 10 minutes

    @BeforeEach
    public void init(){
        cache = new MicroCache<>(
                key -> {},
                key -> {},
                key -> "value for key " + key,
                10);
    }

    @AfterEach
    public void dispose(){
        cache.clear();
        cache.close();
    }

    @Test
    public void getAnUnknowKey(){
        assertEquals("value for key hello", cache.get("hello"));
    }

    @Test
    public void putAndGet(){
        cache.put("hello", "hi", defaultTTLMs);
        assertEquals("hi", cache.get("hello"));
    }

    @Test
    public void removeAndGet(){
        cache.put("hello", "hi", defaultTTLMs);
        cache.remove("hello");
        assertEquals("value for key hello", cache.get("hello"));
    }

    @Test
    public void removeUnknownKey(){
        cache.remove("hello");
        assertEquals(0, cache.size());
    }

    @Test
    public void upsetKey(){
        cache.put("hello", "hi", defaultTTLMs);
        cache.put("hello", "hibis", defaultTTLMs);
        assertEquals("hibis", cache.get("hello"));
    }

    @Test
    public void maxSizeReached(){

        for(int i = 0 ; i < 20 ; ++i)
            cache.put("key"+i, "value"+i, defaultTTLMs);

        assertEquals(10, cache.size());

        for(int i = 10 ; i < 20 ; ++i)
            assertEquals( "value"+i, cache.get("key"+i));
    }

    @Test
    public void cleanupScheduled() throws InterruptedException {

        for(int i = 0 ; i < 20 ; ++i)
            cache.put("key"+i, "value"+i, 100);

        Thread.sleep(200);

        assertEquals(0, cache.size());
    }

}
