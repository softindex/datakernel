package io.datakernel.ot.utils;

import io.datakernel.async.Stages;
import io.datakernel.ot.OTCommit;
import io.datakernel.ot.OTRemote;

import java.util.*;
import java.util.concurrent.CompletionStage;
import java.util.stream.Stream;

import static java.util.Collections.singletonList;
import static java.util.stream.Collectors.toList;
import static java.util.stream.Collectors.toMap;

public class GraphBuilder<K, D> {
	private final OTRemote<K, D> otRemote;
	private final Map<K, Set<KeyDiff<K, D>>> edgesMap = new HashMap<>();
	private final Map<K, K> names = new HashMap<>();
	private final Set<K> finished = new HashSet<>();

	public GraphBuilder(OTRemote<K, D> otRemote) {
		this.otRemote = otRemote;
	}

	public CompletionStage<Map<K, K>> buildGraph(List<Entry<K, D>> edges) {
		for (Entry<K, D> keyDiff : edges) {
			final K left = keyDiff.left;
			edgesMap.computeIfAbsent(left, k -> new HashSet<>());
			edgesMap.computeIfAbsent(keyDiff.right, k -> new HashSet<>()).add(new KeyDiff<>(left, keyDiff.diffs));
		}

		return build(otRemote, edgesMap, names, finished).thenApply($ -> names);
	}

	// TODO: optimize
	private static <K, D> CompletionStage<Void> build(OTRemote<K, D> otRemote, Map<K, Set<KeyDiff<K, D>>> edges,
	                                                  Map<K, K> names, Set<K> finished) {
		return edges.entrySet().stream()
				.filter(entry -> !finished.contains(entry.getKey()))
				.filter(entry -> finished.containsAll(toParents(entry).collect(toList())))
				.findFirst()
				.map(entry -> otRemote.createId()
						.whenComplete(Stages.onResult(id -> names.put(entry.getKey(), id)))
						.thenApply(id -> singletonList(OTCommit.of(id, toDiffs(entry))))
						.thenCompose(otRemote::push)
						.thenAccept($ -> finished.add(entry.getKey()))
						.thenComposeAsync($ -> build(otRemote, edges, names, finished)))
				.orElse(Stages.of(null));
	}

	private static <K, D> Stream<K> toParents(Map.Entry<K, Set<KeyDiff<K, D>>> entry) {
		return entry.getValue().stream().map(KeyDiff::getKey);
	}

	private static <K, D> Map<K, List<D>> toDiffs(Map.Entry<K, Set<KeyDiff<K, D>>> entry) {
		return entry.getValue().stream().collect(toMap(KeyDiff::getKey, KeyDiff::getDiffs));
	}

	public static <K, D> Entry<K, D> edge(K node, K child, List<D> diffs) {
		return new Entry<>(node, child, diffs);
	}

	public static <K, D> Entry<K, D> edge(K node, K child, D diff) {
		return new Entry<>(node, child, singletonList(diff));
	}

	public static class Entry<K, D> {
		public final K left;
		public final K right;
		public final List<D> diffs;

		public Entry(K left, K right, List<D> diffs) {
			this.left = left;
			this.right = right;
			this.diffs = diffs;
		}

	}

	public static class KeyDiff<K, D> {
		public final K key;
		public final List<D> diffs;

		private KeyDiff(K key, List<D> diffs) {
			this.key = key;
			this.diffs = diffs;
		}

		public K getKey() {
			return key;
		}

		public List<D> getDiffs() {
			return diffs;
		}

		@Override
		public String toString() {
			return "KeyDiff{" +
					"key=" + key +
					", diffs=" + diffs +
					'}';
		}

		@Override
		public boolean equals(Object o) {
			if (this == o) return true;
			if (o == null || getClass() != o.getClass()) return false;

			KeyDiff<?, ?> keyDiff = (KeyDiff<?, ?>) o;

			return key != null ? key.equals(keyDiff.key) : keyDiff.key == null;
		}

		@Override
		public int hashCode() {
			return key != null ? key.hashCode() : 0;
		}
	}
}
