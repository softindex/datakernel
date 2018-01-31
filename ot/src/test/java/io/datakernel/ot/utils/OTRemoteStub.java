package io.datakernel.ot.utils;

import io.datakernel.annotation.Nullable;
import io.datakernel.async.Stage;
import io.datakernel.ot.OTCommit;
import io.datakernel.ot.OTRemote;

import java.util.*;

import static io.datakernel.util.CollectionUtils.difference;

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

	OTRemoteStub(Sequence<K> sequence, Comparator<K> comparator, TreeSet<K> nodes) {
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
	public Stage<K> createCommitId() {
		revisionId = sequence.next(revisionId);
		return Stage.of(revisionId);
	}

	@Override
	public Stage<Void> push(Collection<OTCommit<K, D>> commits) {
		for (OTCommit<K, D> commit : commits) {
			K to = commit.getId();
			if (commit.isRoot()) nodes.add(commit.getId());

			for (K from : commit.getParents().keySet()) {
				List<D> diffs = commit.getParents().get(from);
				add(from, to, diffs);
			}
			if (commit.isRoot() && root == null) root = commit.getId();
		}
		return Stage.of(null);
	}

	public void add(K from, K to, List<? extends D> diffs) {
		nodes.add(from);
		nodes.add(to);
		forward.computeIfAbsent(from, $ -> new HashMap<>()).put(to, diffs);
		backward.computeIfAbsent(to, $ -> new HashMap<>()).put(from, diffs);
	}

	public void push(OTCommit<K, D> commit) {
		push(Collections.singletonList(commit));
	}

	@Override
	public Stage<Set<K>> getHeads() {
		return Stage.of(difference(nodes, forward.keySet()));
	}

	@Override
	public Stage<OTCommit<K, D>> loadCommit(K revisionId) {
		if (!nodes.contains(revisionId))
			throw new IllegalArgumentException("id=" + revisionId);
		Map<K, List<? extends D>> parentDiffs = backward.get(revisionId);
		if (parentDiffs == null) {
			parentDiffs = Collections.emptyMap();
		}
		return Stage.of(OTCommit.of(revisionId, parentDiffs));
	}

	@Override
	public Stage<Void> saveSnapshot(K revisionId, List<D> diffs) {
		snapshots.put(revisionId, diffs);
		return Stage.of(null);
	}

	@Override
	public Stage<List<D>> loadSnapshot(K revisionId) {
		return snapshots.containsKey(revisionId)
				? Stage.of(snapshots.get(revisionId))
				: Stage.of(Collections.emptyList());

	}

	@Override
	public Stage<Void> cleanup(K revisionId) {
		throw new UnsupportedOperationException();
	}

	@Override
	public Stage<Void> backup(K revisionId, List<D> diffs) {
		throw new UnsupportedOperationException();
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
