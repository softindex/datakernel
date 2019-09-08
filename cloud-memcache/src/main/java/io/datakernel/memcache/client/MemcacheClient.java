package io.datakernel.memcache.client;

import io.datakernel.promise.Promise;

public interface MemcacheClient<K, V> {

	Promise<Void> put(K key, V value, int timeout);

	Promise<V> get(K key, int timeout);

	default Promise<Void> put(K key, V value) {
		return put(key, value, Integer.MAX_VALUE);
	}

	default Promise<V> get(K key) {
		return get(key, Integer.MAX_VALUE);
	}
}
