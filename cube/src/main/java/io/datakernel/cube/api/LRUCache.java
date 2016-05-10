/*
 * Copyright (C) 2015 SoftIndex LLC.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.datakernel.cube.api;

import com.google.common.collect.ImmutableMap;

import java.util.LinkedHashMap;
import java.util.Map;

public final class LRUCache<K, V> implements LRUCacheMBean {
	private static class LRUMap<K, V> extends LinkedHashMap<K, V> {
		private int cacheSize;

		public LRUMap(int cacheSize) {
			super(16, 0.75f, true);
			this.cacheSize = cacheSize;
		}

		protected boolean removeEldestEntry(Map.Entry<K, V> eldest) {
			return size() > cacheSize;
		}
	}

	private final LRUMap<K, V> cache;

	// stats
	private int requestsCount;
	private int missesCount;
	private int putCount;

	public LRUCache(int cacheSize) {
		cache = new LRUMap<>(cacheSize);
	}

	public synchronized V get(K key) {
		++requestsCount;
		V v = cache.get(key);
		if (v == null) {
			++missesCount;
		}
		return v;
	}

	public synchronized V put(K key, V value) {
		++putCount;
		return cache.put(key, value);
	}

	public synchronized V remove(K key) {
		return cache.remove(key);
	}

	public Map<K, V> asMap() {
		return ImmutableMap.copyOf(cache);
	}

	// stats
	@Override
	public int getCurrentCacheSize() {
		return cache.size();
	}

	@Override
	public int getCacheRequestsCount() {
		return requestsCount;
	}

	@Override
	public int getCachePutCount() {
		return putCount;
	}

	@Override
	public double getCacheHitRate() {
		return ((double) requestsCount - missesCount) / requestsCount;
	}

	@Override
	public Map<String, String> getCacheContents() {
		Map<String, String> map = new LinkedHashMap<>(cache.size());

		for (Map.Entry<K, V> entry : cache.entrySet()) {
			map.put(entry.getKey().toString(), entry.getValue().toString());
		}

		return map;
	}
}