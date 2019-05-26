package io.datakernel.di.util;

import org.jetbrains.annotations.Nullable;

import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.function.BiConsumer;
import java.util.function.Function;
import java.util.stream.Stream;

import static java.util.Collections.emptyMap;

public final class Trie<K, V> {
	private final V payload;
	private final Map<K, Trie<K, V>> children;

	public Trie(V payload, Map<K, Trie<K, V>> children) {
		this.payload = payload;
		this.children = children;
	}

	public static <K, V> Trie<K, V> leaf(V value) {
		return new Trie<>(value, new HashMap<>());
	}

	public static <K, V> Trie<K, V> of(V payload, Map<K, Trie<K, V>> children) {
		return new Trie<>(payload, children);
	}

	public V get() {
		return payload;
	}

	public Map<K, Trie<K, V>> getChildren() {
		return children;
	}

	@Nullable
	public Trie<K, V> get(K key) {
		return children.get(key);
	}

	public Trie<K, V> getOrDefault(K key, V defaultValue) {
		return children.getOrDefault(key, new Trie<>(defaultValue, emptyMap()));
	}

	public Trie<K, V> computeIfAbsent(K key, Function<K, V> f) {
		return children.computeIfAbsent(key, k -> leaf(f.apply(k)));
	}

	@Nullable
	public Trie<K, V> get(K[] path) {
		Trie<K, V> subtree = this;
		for (K key : path) {
			subtree = subtree.get(key);
			if (subtree == null) {
				return null;
			}
		}
		return subtree;
	}

	public Trie<K, V> computeIfAbsent(K[] path, Function<K, V> f) {
		Trie<K, V> subtree = this;
		for (K key : path) {
			subtree = subtree.computeIfAbsent(key, f);
		}
		return subtree;
	}

	public void addAll(Trie<K, V> other, BiConsumer<V, V> merger) {
		mergeInto(this, other, merger);
	}

	public <E> Trie<K, E> map(Function<? super V, ? extends E> fn) {
		Trie<K, E> root = leaf(fn.apply(payload));
		children.forEach((k, sub) -> root.getChildren().put(k, sub.map(fn)));
		return root;
	}

	private static <K, V> void mergeInto(Trie<K, V> into, Trie<K, V> from, BiConsumer<V, V> merger) {
		if (into == from) {
			return;
		}
		merger.accept(into.get(), from.get());
		from.children.forEach((scope, child) -> mergeInto(into.children.computeIfAbsent(scope, $ -> child), child, merger));
	}

	public static <K, V> Trie<K, V> merge(BiConsumer<V, V> merger, V rootPayload, Trie<K, V> first, Trie<K, V> second) {
		Trie<K, V> combined = leaf(rootPayload);
		mergeInto(combined, first, merger);
		mergeInto(combined, second, merger);
		return combined;
	}

	@SafeVarargs
	public static <K, V> Trie<K, V> merge(BiConsumer<V, V> merger, V rootPayload, Trie<K, V> first, Trie<K, V> second, Trie<K, V>... rest) {
		return merge(merger, rootPayload, Stream.concat(Stream.of(first, second), Arrays.stream(rest)));
	}

	public static <K, V> Trie<K, V> merge(BiConsumer<V, V> merger, V rootPayload, Collection<Trie<K, V>> bindings) {
		return merge(merger, rootPayload, bindings.stream());
	}

	public static <K, V> Trie<K, V> merge(BiConsumer<V, V> merger, V rootPayload, Stream<Trie<K, V>> bindings) {
		Trie<K, V> combined = leaf(rootPayload);
		bindings.forEach(sb -> mergeInto(combined, sb, merger));
		return combined;
	}

	@Override
	public String toString() {
		return "Trie{payload=" + payload +", children=" + children + '}';
	}
}
