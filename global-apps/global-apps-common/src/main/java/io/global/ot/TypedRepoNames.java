package io.global.ot;

import io.datakernel.di.core.Key;
import org.jetbrains.annotations.Nullable;

import java.util.HashMap;
import java.util.Map;

public final class TypedRepoNames {
	private final String prefix;
	private final Map<Key<?>, String> repoNames = new HashMap<>();
	private final Map<String, Key<?>> repoNamesReverse = new HashMap<>();

	private final Map<Key<?>, String> repoPrefixes = new HashMap<>();
	private final Map<String, Key<?>> repoPrefixesReverse = new HashMap<>();

	private TypedRepoNames(String prefix) {
		this.prefix = prefix;
	}

	public static TypedRepoNames create(String prefix) {
		return new TypedRepoNames(prefix + '/');
	}

	public static TypedRepoNames create() {
		return new TypedRepoNames("");
	}

	public TypedRepoNames withRepoName(Key<?> key, String repoName) {
		repoNames.put(key, prefix + repoName);
		repoNamesReverse.put(repoName, key);
		return this;
	}

	public TypedRepoNames withGlobalRepoName(Key<?> key, String globalRepoName) {
		repoNames.put(key, globalRepoName);
		repoNamesReverse.put(globalRepoName, key);
		return this;
	}

	public TypedRepoNames withRepoPrefix(Key<?> key, String repoPrefix) {
		repoPrefixes.put(key, prefix + repoPrefix + '/');
		repoPrefixesReverse.put(repoPrefix, key);
		return this;
	}

	public String getPrefix() {
		return prefix;
	}

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

	@Nullable
	public Key<?> getKeyByRepo(String repoName) {
		Key<?> plain = repoNamesReverse.get(repoName.startsWith(prefix) ? repoName.substring(prefix.length()) : repoName);
		if (plain != null) {
			return plain;
		}
		return repoPrefixesReverse.get(repoName.substring(prefix.length(), repoName.lastIndexOf('/')));
	}

	public boolean hasRepoName(Key<?> key) {
		return repoNames.containsKey(key);
	}

	@Override
	public String toString() {
		return "TypedRepoNames{prefix='" + prefix + "', repoNames=" + repoNames + ", repoPrefixes=" + repoPrefixes + '}';
	}
}
