package io.global.comm.util;

import io.datakernel.async.Promise;

import java.util.List;
import java.util.Map;

public interface PagedAsyncMap<K, V> {
	Promise<Map<K, V>> get();

	Promise<V> get(K key);

	Promise<Void> put(K key, V value);

	Promise<Void> remove(K key);

	Promise<Integer> size();

	Promise<List<Map.Entry<K, V>>> slice(int offset, int size);
}
