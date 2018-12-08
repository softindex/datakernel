package io.datakernel.loader.cache;

import java.util.concurrent.ConcurrentHashMap;

public final class CacheNoEvictionPolicy implements Cache {

    private final ConcurrentHashMap<String, byte[]> map;

    private CacheNoEvictionPolicy() {
        map = new ConcurrentHashMap<>();
    }

    public static CacheNoEvictionPolicy create() {
        return new CacheNoEvictionPolicy();
    }

    @Override
    public byte[] get(String key) {
        return map.get(key);
    }

    @Override
    public void put(String key, byte[] value) {
        map.put(key, value);
    }
}