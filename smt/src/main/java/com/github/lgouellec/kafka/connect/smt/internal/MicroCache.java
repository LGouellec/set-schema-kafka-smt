package com.github.lgouellec.kafka.connect.smt.internal;

import org.slf4j.LoggerFactory;

import java.io.*;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.*;
import java.util.function.Consumer;
import java.util.function.Function;

public class MicroCache<K, V> {
    @Serial
    private static final long serialVersionUID = 1L;
    private static final org.slf4j.Logger LOGGER = LoggerFactory.getLogger(SchemaCache.class);
    private final ConcurrentHashMap<K, CacheEntry<V>> cache = new ConcurrentHashMap<>();
    private final LinkedHashMap<K, V> lruCache;
    private transient final ScheduledExecutorService cleaner = Executors.newScheduledThreadPool(Runtime.getRuntime().availableProcessors());
    private transient final ExecutorService asyncLoader = Executors.newCachedThreadPool();
    private transient final Consumer<K> expiryCallback;
    private transient final Consumer<K> loadingCallback;
    private transient final Function<K, V> asyncLoaderFunction;
    private final int maxSize;

    public MicroCache(Consumer<K> expiryCallback, Consumer<K> loadingCallback, Function<K, V> asyncLoaderFunction, int maxSize) {
        this.expiryCallback = expiryCallback;
        this.loadingCallback = loadingCallback;
        this.asyncLoaderFunction = asyncLoaderFunction;
        this.maxSize = maxSize;
        this.lruCache = new LinkedHashMap<>(maxSize, 0.75f, true) {
            protected boolean removeEldestEntry(Map.Entry<K, V> eldest) {
                return size() > maxSize;
            }
        };
    }

    public void put(K key, V value, long ttlMillis) {
        synchronized (lruCache) {
            if (lruCache.size() >= maxSize) {
                Iterator<K> iterator = lruCache.keySet().iterator();
                if (iterator.hasNext()) {
                    K eldestKey = iterator.next();
                    lruCache.remove(eldestKey);
                    cache.remove(eldestKey);
                }
            }
            lruCache.put(key, value);
        }
        long expiryTime = System.currentTimeMillis() + ttlMillis;
        cache.put(key, new CacheEntry<>(value, expiryTime));
        scheduleCleanup(key, ttlMillis);
    }

    public V get(K key) {
        CacheEntry<V> entry = cache.get(key);
        if (entry != null && System.currentTimeMillis() < entry.expiryTime) {
            synchronized (lruCache) {
                lruCache.put(key, entry.value);
            }
            return entry.value;
        }
        cache.remove(key);
        return loadAsync(key);
    }

    private V loadAsync(K key) {
        CompletableFuture<V> future = CompletableFuture.supplyAsync(
                () -> {
                    loadingCallback.accept(key);
                    return asyncLoaderFunction.apply(key);
                },
                asyncLoader);
        try {
            return future.get();
        } catch (Exception e) {
            LOGGER.warn("Async loading failed for key: " + key);
            return null;
        }
    }

    public void remove(K key) {
        cache.remove(key);
        synchronized (lruCache) {
            lruCache.remove(key);
        }
    }

    public void clear() {
        cache.clear();
        synchronized (lruCache) {
            lruCache.clear();
        }
    }

    public void close(){
        cleaner.shutdownNow();
        asyncLoader.shutdownNow();
    }

    public int size() {
        return cache.size();
    }

    private void scheduleCleanup(K key, long ttlMillis) {
        cleaner.schedule(() -> {
            CacheEntry<V> entry = cache.get(key);
            if (entry != null && System.currentTimeMillis() >= entry.expiryTime) {
                cache.remove(key);
                synchronized (lruCache) {
                    lruCache.remove(key);
                }
                expiryCallback.accept(key);
                LOGGER.info("Entry expired and removed: " + key);
            }
        }, ttlMillis, TimeUnit.MILLISECONDS);
    }

    private static class CacheEntry<V> implements Serializable {
        @Serial
        private static final long serialVersionUID = 1L;
        final V value;
        final long expiryTime;

        CacheEntry(V value, long expiryTime) {
            this.value = value;
            this.expiryTime = expiryTime;
        }
    }
}