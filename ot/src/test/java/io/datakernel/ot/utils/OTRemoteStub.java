package io.datakernel.ot.utils;

import io.datakernel.annotation.Nullable;
import io.datakernel.async.Stages;
import io.datakernel.ot.OTCommit;
import io.datakernel.ot.OTRemote;
import io.datakernel.util.CollectionUtils;

import java.util.*;
import java.util.concurrent.CompletionStage;

public final class OTRemoteStub<K, D> implements OTRemote<K, D> {
	public interface Sequence<K> {
		K next(@Nullable K prev);
	}

	public static class IntegerSequence implements Sequence<Integer> {
		@Override
		public Integer next(Integer prev) {
			return (prev != null) ? (prev + 1) : 0;
		}
	}

	public static class TestSequence<K> implements Sequence<K> {
		final Iterator<K> iterator;

		private TestSequence(Iterator<K> iterator) {
			this.iterator = iterator;
		}

		public static <K> TestSequence<K> of(Iterable<K> iterable) {
			return new TestSequence<K>(iterable.iterator());
		}

		public static <K> TestSequence<K> of(K... iterable) {
			return of(Arrays.asList(iterable));
		}

		@Override
		public K next(K prev) {
			return iterator.next();
		}
	}

	public final Sequence<K> sequence;
	public final Comparator<K> comparator;

	public K revisionId;
	public K root;

	public final TreeSet<K> nodes;
	public final Map<K, Map<K, List<? extends D>>> forward = new LinkedHashMap<>();
	public final Map<K, Map<K, List<? extends D>>> backward = new LinkedHashMap<>();
	public final Map<K, List<D>> snapshots = new LinkedHashMap<>();

	private OTRemoteStub(Sequence<K> sequence, Comparator<K> comparator, TreeSet<K> nodes) {
		this.sequence = sequence;
		this.comparator = comparator;
		this.nodes = nodes;
	}

	public static <K, D> OTRemoteStub<K, D> create(Sequence<K> sequence, Comparator<K> comparator) {
		OTRemoteStub<K, D> result = new OTRemoteStub<>(sequence, comparator, new TreeSet<>(comparator));
//		K revisionId = sequence.next(null);
//		result.push(OTCommit.<K, D>ofRoot(revisionId));
		return result;
	}

	public static <K, D> OTRemoteStub<K, D> create(Comparator<K> comparator) {
		return new OTRemoteStub<>(null, comparator, new TreeSet<>(comparator));
	}

	@Override
	public CompletionStage<K> createId() {
		revisionId = sequence.next(revisionId);
		return Stages.of(revisionId);
	}

	@Override
	public CompletionStage<Void> push(Collection<OTCommit<K, D>> commits) {
		for (OTCommit<K, D> commit : commits) {
			K to = commit.getId();
			if (commit.isRoot()) nodes.add(commit.getId());

			for (K from : commit.getParents().keySet()) {
				List<D> diffs = commit.getParents().get(from);
				add(from, to, diffs);
			}
			if (commit.isRoot() && root == null) root = commit.getId();
		}
		return Stages.of(null);
	}

	public void add(K from, K to, List<? extends D> diffs) {
		nodes.add(from);
		nodes.add(to);
		forward.computeIfAbsent(from, CollectionUtils.$::newHashMap).put(to, diffs);
		backward.computeIfAbsent(to, CollectionUtils.$::newHashMap).put(from, diffs);
	}

	public void push(OTCommit<K, D> commit) {
		push(Collections.singletonList(commit));
	}

	@Override
	public CompletionStage<Set<K>> getHeads() {
		return Stages.of(difference(nodes, forward.keySet()));
	}

	public static <T> Set<T> difference(Set<T> a, Set<T> b) {
		Set<T> set = new HashSet<>(a);
		set.removeAll(b);
		return set;
	}

	@Override
	public CompletionStage<OTCommit<K, D>> loadCommit(K revisionId) {
		if (!nodes.contains(revisionId))
			throw new IllegalArgumentException("id=" + revisionId);
		Map<K, List<? extends D>> parentDiffs = backward.get(revisionId);
		if (parentDiffs == null) {
			parentDiffs = Collections.emptyMap();
		}
		return Stages.of(OTCommit.of(revisionId, parentDiffs));
	}

	@Override
	public CompletionStage<Void> saveSnapshot(K revisionId, List<D> diffs) {
		snapshots.put(revisionId, diffs);
		return Stages.of(null);
	}

	@Override
	public CompletionStage<List<D>> loadSnapshot(K revisionId) {
		return snapshots.containsKey(revisionId)
				? Stages.of(snapshots.get(revisionId))
				: Stages.ofException(new IllegalArgumentException());

	}

	@Override
	public CompletionStage<Boolean> isSnapshot(K revisionId) {
		return Stages.of(snapshots.containsKey(revisionId));
	}

	@Override
	public String toString() {
		StringBuilder sb = new StringBuilder();
		String separator = "";
		for (K node : nodes) {
			sb.append(separator).append(loadCommit(node).toString());
			separator = "\n";
		}
		return sb.toString();
	}
}
