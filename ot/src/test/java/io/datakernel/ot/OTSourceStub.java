package io.datakernel.ot;

import com.google.common.collect.Sets;
import io.datakernel.annotation.Nullable;
import io.datakernel.async.SettableStage;

import java.util.*;
import java.util.concurrent.CompletionStage;

import static io.datakernel.async.SettableStage.immediateStage;
import static io.datakernel.ot.OTUtils.ensureMapValue;

public final class OTSourceStub<K, D> implements OTRemote<K, D> {
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

	public static final IntegerSequence INTEGER_SEQUENCE = new IntegerSequence();

	public final Sequence<K> sequence;

	public K revisionId;

	public final TreeSet<K> nodes;
	public final Map<K, Map<K, List<? extends D>>> forward = new LinkedHashMap<>();
	public final Map<K, Map<K, List<? extends D>>> backward = new LinkedHashMap<>();

	public final TreeMap<K, List<D>> checkpoints = new TreeMap<>();

	private OTSourceStub(Sequence<K> sequence, TreeSet<K> nodes) {
		this.sequence = sequence;
		this.nodes = nodes;
	}

	public static <K, D> OTSourceStub<K, D> create(Sequence<K> sequence, Comparator<K> comparator) {
		OTSourceStub<K, D> result = new OTSourceStub<>(sequence, new TreeSet<K>(comparator));
//		K revisionId = sequence.next(null);
//		result.push(OTCommit.<K, D>ofRoot(revisionId));
		return result;
	}

	public static <K, D> OTSourceStub<K, D> create(Comparator<K> comparator) {
		return new OTSourceStub<>(null, new TreeSet<K>(comparator));
	}

	@Override
	public CompletionStage<K> createId() {
		revisionId = sequence.next(revisionId);
		return immediateStage(revisionId);
	}

	@Override
	public CompletionStage<Void> push(List<OTCommit<K, D>> commits) {
		for (OTCommit<K, D> commit : commits) {
			K to = commit.getId();
			for (K from : commit.getParents().keySet()) {
				List<D> diffs = commit.getParents().get(from);
				add(from, to, diffs);
			}
			if (commit.isCheckpoint()) {
				List<D> checkpoint = commit.getCheckpoint();
				checkpoints.put(to, checkpoint != null ? checkpoint : Collections.<D>emptyList());
			}
		}
		return immediateStage(null);
	}

	public void add(K from, K to, List<? extends D> diffs) {
		nodes.add(from);
		nodes.add(to);
		ensureMapValue(forward, from).put(to, diffs);
		ensureMapValue(backward, to).put(from, diffs);
	}

	public void push(OTCommit<K, D> commit) {
		push(Collections.singletonList(commit));
	}

	@Override
	public CompletionStage<Set<K>> getHeads() {
		return immediateStage(Sets.difference(nodes, forward.keySet()));
	}

	@Override
	public CompletionStage<K> getCheckpoint() {
		return immediateStage(checkpoints.lastKey());
	}

	@Override
	public SettableStage<OTCommit<K, D>> loadCommit(K revisionId) {
		if (!nodes.contains(revisionId))
			throw new IllegalArgumentException("id="+ revisionId);
		Map<K, List<? extends D>> parentDiffs = backward.get(revisionId);
		if (parentDiffs == null) {
			parentDiffs = Collections.emptyMap();
		}
		return immediateStage(OTCommit.of(revisionId, checkpoints.get(revisionId), parentDiffs));
	}

	@Override
	public String toString() {
		StringBuilder sb = new StringBuilder();
		String separator = "";
		for (K node : nodes) {
			sb.append(separator + loadCommit(node).toString());
			separator = "\n";
		}
		return sb.toString();
	}
}
