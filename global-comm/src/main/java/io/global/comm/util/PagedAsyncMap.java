package io.global.comm.util;

import io.datakernel.promise.Promise;

import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

public interface PagedAsyncMap<K, V> {
	Promise<Map<K, V>> get();

	Promise<V> get(K key);

	Promise<Void> put(K key, V value);

	Promise<Void> remove(K key);

	Promise<Integer> size();

	Promise<List<Entry<K, V>>> slice(int offset, int size);
}
