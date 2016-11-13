package io.datakernel.cube;

import io.datakernel.codegen.DefiningClassLoader;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;

public class CubeClassLoaderCache implements CubeClassLoaderCacheMBean {
	static final class Key {
		final Set<String> attributes;
		final Set<String> measures;
		final Set<String> filterDimensions;

		Key(Set<String> attributes, Set<String> measures, Set<String> filterDimensions) {
			this.attributes = attributes;
			this.measures = measures;
			this.filterDimensions = filterDimensions;
		}

		@Override
		public boolean equals(Object o) {
			if (this == o) return true;
			if (o == null || getClass() != o.getClass()) return false;

			Key that = (Key) o;

			if (!attributes.equals(that.attributes)) return false;
			if (!measures.equals(that.measures)) return false;
			if (!filterDimensions.equals(that.filterDimensions)) return false;
			return true;
		}

		@Override
		public int hashCode() {
			int result = attributes.hashCode();
			result = 31 * result + measures.hashCode();
			result = 31 * result + filterDimensions.hashCode();
			return result;
		}

		@Override
		public String toString() {
			return "{" + attributes + ", " + measures + ", " + filterDimensions + '}';
		}
	}

	private final DefiningClassLoader rootClassLoader;
	private final LinkedHashMap<Key, DefiningClassLoader> cache = new LinkedHashMap<Key, DefiningClassLoader>(16, 0.75f, false) {
		protected boolean removeEldestEntry(Map.Entry<Key, DefiningClassLoader> eldest) {
			return size() > targetCacheKeys;
		}
	};

	private int targetCacheKeys;

	// JMX
	private int cacheRequests;
	private int cacheMisses;

	private CubeClassLoaderCache(DefiningClassLoader rootClassLoader, int targetCacheKeys) {
		this.rootClassLoader = rootClassLoader;
		this.targetCacheKeys = targetCacheKeys;
	}

	public static CubeClassLoaderCache create(DefiningClassLoader root, int cacheSize) {
		return new CubeClassLoaderCache(root, cacheSize);
	}

	public synchronized DefiningClassLoader getOrCreate(Key key) {
		cacheRequests++;
		DefiningClassLoader classLoader = cache.get(key);
		if (classLoader == null) {
			cacheMisses++;
			classLoader = DefiningClassLoader.create(rootClassLoader);
			cache.put(key, classLoader);
		}
		return classLoader;
	}

	// JMX
	@Override
	synchronized public void clear() {
		cache.clear();
	}

	@Override
	public int getTargetCacheKeys() {
		return targetCacheKeys;
	}

	@Override
	public void setTargetCacheKeys(int targetCacheKeys) {
		this.targetCacheKeys = targetCacheKeys;
	}

	@Override
	synchronized public int getDefinedClassesCount() {
		int result = 0;
		for (DefiningClassLoader classLoader : cache.values()) {
			result += classLoader.getDefinedClassesCount();
		}
		return result;
	}

	@Override
	synchronized public int getDefinedClassesCountMaxPerKey() {
		int result = 0;
		for (DefiningClassLoader classLoader : cache.values()) {
			result = Math.max(result, classLoader.getDefinedClassesCount());
		}
		return result;
	}

	@Override
	public int getCacheKeys() {
		return targetCacheKeys;
	}

	@Override
	public int getCacheRequests() {
		return cacheRequests;
	}

	@Override
	public int getCacheMisses() {
		return cacheMisses;
	}

	@Override
	public Map<String, String> getCacheContents() {
		Map<String, String> map;
		synchronized (this) {
			map = new LinkedHashMap<>(cache.size());
		}

		for (Map.Entry<Key, DefiningClassLoader> entry : cache.entrySet()) {
			map.put(entry.getKey().toString(), entry.getValue().toString());
		}

		return map;
	}
}
