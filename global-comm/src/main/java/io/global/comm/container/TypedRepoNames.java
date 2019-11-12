package io.global.comm.container;

import io.datakernel.di.core.Key;
import org.jetbrains.annotations.Nullable;

import java.util.HashMap;
import java.util.Map;

public final class TypedRepoNames {
	private final String prefix;
	private final Map<Key<?>, String> repoNames = new HashMap<>();
	private final Map<Key<?>, String> repoPrefixes = new HashMap<>();

	private TypedRepoNames(String prefix) {
		this.prefix = prefix + '/';
	}

	public static TypedRepoNames create(String prefix) {
		return new TypedRepoNames(prefix);
	}

	public TypedRepoNames withRepoName(Key<?> key, String repoName) {
		repoNames.put(key, prefix + repoName);
		return this;
	}

	public TypedRepoNames withRepoPrefix(Key<?> key, String repoPrefix) {
		repoPrefixes.put(key, prefix + repoPrefix + '/');
		return this;
	}

	@Nullable
	public String getRepoName(Key<?> key) {
		String repoName = repoNames.get(key);
		if (repoName == null) {
			throw new IllegalArgumentException("No repo name for type " + key.getDisplayString() + " provided");
		}
		return repoName;
	}

	public String getRepoPrefix(Key<?> key) {
		String repoPrefix = repoPrefixes.get(key);
		if (repoPrefix == null) {
			throw new IllegalArgumentException("No repo prefix for type " + key.getDisplayString() + " provided");
		}
		return repoPrefix;
	}

	@Override
	public String toString() {
		return "TypedRepoNames{prefix='" + prefix + "', repoNames=" + repoNames + ", repoPrefixes=" + repoPrefixes + '}';
	}
}
