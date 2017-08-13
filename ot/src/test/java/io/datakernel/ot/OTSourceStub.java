package io.datakernel.ot;

import com.google.common.collect.Sets;
import io.datakernel.annotation.Nullable;
import io.datakernel.async.CompletionCallback;
import io.datakernel.async.ResultCallback;

import java.util.*;

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

	public static class ProvidedSequence<K> implements Sequence<K> {
		final Iterator<K> iterator;

		private ProvidedSequence(Iterator<K> iterator) {
			this.iterator = iterator;
		}

		public static <K> ProvidedSequence<K> of(Iterable<K> iterable) {
			return new ProvidedSequence<K>(iterable.iterator());
		}

		public static <K> ProvidedSequence<K> of(K... iterable) {
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
	public void createId(ResultCallback<K> callback) {
		callback.setResult(createId());
	}

	public K createId() {
		revisionId = sequence.next(revisionId);
		return revisionId;
	}

	@Override
	public void push(List<OTCommit<K, D>> otCommits, CompletionCallback callback) {
		push(otCommits);
		callback.setComplete();
	}

	public void push(List<OTCommit<K, D>> commits) {
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
	public void getHeads(ResultCallback<Set<K>> callback) {
		callback.setResult(getHeads());
	}

	public Set<K> getHeads() {
		return Sets.difference(nodes, forward.keySet());
	}

	@Override
	public void getCheckpoint(ResultCallback<K> callback) {
		callback.setResult(getCheckpoint());
	}

	public K getCheckpoint() {
		return checkpoints.lastKey();
	}

	@Override
	public void loadCommit(K revisionId, ResultCallback<OTCommit<K, D>> callback) {
		callback.setResult(loadCommit(revisionId));
	}

	public OTCommit<K, D> loadCommit(K revisionId) {
		if (!nodes.contains(revisionId))
			return null;
		Map<K, List<? extends D>> parentDiffs = backward.get(revisionId);
		if (parentDiffs == null) {
			parentDiffs = Collections.emptyMap();
		}
		return OTCommit.of(revisionId, checkpoints.get(revisionId), parentDiffs);
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
