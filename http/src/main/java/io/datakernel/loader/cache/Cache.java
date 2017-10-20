package io.datakernel.loader.cache;

public interface Cache {

    byte[] get(String key);

    void put(String key, byte[] value);

}