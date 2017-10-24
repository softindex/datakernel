package io.datakernel.ot;

import com.google.common.base.Predicate;
import io.datakernel.annotation.Nullable;
import io.datakernel.async.CompletionCallback;
import io.datakernel.eventloop.Eventloop;
import io.datakernel.eventloop.EventloopService;
import io.datakernel.ot.OTUtils.FindResult;

import java.util.*;
import java.util.concurrent.CompletionStage;

import static io.datakernel.async.AsyncCallbacks.forwardTo;
import static io.datakernel.async.SettableStage.immediateFailedStage;
import static io.datakernel.async.SettableStage.immediateStage;
import static java.util.Collections.singleton;
import static java.util.Collections.singletonList;

public final class OTStateManager<K, D> implements EventloopService {

	private final Eventloop eventloop;
	private final OTSystem<D> otSystem;
	private final OTRemote<K, D> source;
	private final Comparator<K> comparator;

	private K head;
	private List<D> workingDiffs = new ArrayList<>();
	private LinkedHashMap<K, List<D>> pendingCommits = new LinkedHashMap<>();
	private OTState<D> state;

	public OTStateManager(Eventloop eventloop, OTSystem<D> otSystem, OTRemote<K, D> source, Comparator<K> comparator, OTState<D> state) {
		this.eventloop = eventloop;
		this.otSystem = otSystem;
		this.source = source;
		this.comparator = comparator;
		this.state = state;
	}

	public OTState<D> getState() {
		return state;
	}

	@Override
	public Eventloop getEventloop() {
		return eventloop;
	}

	@Override
	public void start(CompletionCallback callback) {
		checkout()
				.thenCompose($ -> pull())
				.whenComplete(forwardTo(callback));
	}

	@Override
	public void stop(CompletionCallback callback) {
		invalidateInternalState();
		callback.setComplete();
	}

	public CompletionStage<Void> checkout() {
		head = null;
		workingDiffs.clear();
		pendingCommits.clear();
		state.init();
		return source.getCheckpoint().thenCompose(checkpointId ->
				source.loadCommit(checkpointId).thenAccept(commit -> {
					head = checkpointId;
					apply(commit.getCheckpoint());
				}));
	}

	public CompletionStage<Void> checkout(K commitId) {
		head = null;
		workingDiffs.clear();
		pendingCommits.clear();
		state.init();
		return findCheckpoint(singleton(commitId), null).thenCompose(findResult -> {
			if (!findResult.isFound()) {
				return immediateFailedStage(new IllegalStateException("Could not find path to original head"));
			}
			List<D> pathToNewHead = findResult.getParentToChild();
			apply(findResult.getParentCommit().getCheckpoint());
			apply(pathToNewHead);
			head = findResult.getChild();
			return immediateStage(null);
		});
	}

	public CompletionStage<Void> pull() {
		if (!pendingCommits.isEmpty()) {
			return immediateStage(null);
		}
		return source.getHeads().thenCompose(newHeads -> {
			Predicate<OTCommit<K, D>> isHead = input -> input.getId().equals(head);
			return findParent(newHeads, head, isHead).thenCompose(findResult -> {
				if (!findResult.isFound()) {
					return immediateFailedStage(new IllegalStateException("Could not find path to original head"));
				}
				if (!pendingCommits.isEmpty()) {
					return immediateStage(null);
				}
				List<D> pathToNewHead = findResult.getParentToChild();
				TransformResult<D> transformed = otSystem.transform(otSystem.squash(workingDiffs), otSystem.squash(pathToNewHead));
				apply(transformed.left);
				workingDiffs = new ArrayList<>(transformed.right);
				head = findResult.getChild();
				return immediateStage(null);
			});
		});
	}

	public void reset() {
		List<D> diffs = new ArrayList<>();
		for (List<D> ds : pendingCommits.values()) {
			diffs.addAll(ds);
		}
		diffs.addAll(workingDiffs);
		diffs = otSystem.invert(diffs);
		apply(diffs);
		pendingCommits = new LinkedHashMap<>();
		workingDiffs = new ArrayList<>();
	}

	public CompletionStage<K> commitAndPush() {
		return commit().thenCompose(id -> push().thenApply($ -> id));
	}

	public CompletionStage<K> commit() {
		if (workingDiffs.isEmpty()) {
			return immediateStage(null);
		}
		return source.createId().whenComplete((newId, throwable) -> {
			if (throwable == null) {
				pendingCommits.put(newId, otSystem.squash(workingDiffs));
				workingDiffs = new ArrayList<>();
			}
		});
	}

	public CompletionStage<Void> push() {
		if (pendingCommits.isEmpty()) {
			return immediateStage(null);
		}
		K parent = head;
		List<OTCommit<K, D>> list = new ArrayList<>();
		for (Map.Entry<K, List<D>> pendingCommitEntry : pendingCommits.entrySet()) {
			K key = pendingCommitEntry.getKey();
			List<D> diffs = pendingCommitEntry.getValue();
			OTCommit<K, D> commit = OTCommit.ofCommit(key, parent, diffs);
			list.add(commit);
			parent = key;
		}
		return source.push(list).thenAccept($ -> {
			list.forEach(commit -> pendingCommits.remove(commit.getId()));
			head = list.get(list.size() - 1).getId();
		});
	}

	public void add(D diff) {
		add(singletonList(diff));
	}

	public void add(List<D> diffs) {
		try {
			for (D diff : diffs) {
				if (!otSystem.isEmpty(diff)) {
					workingDiffs.add(diff);
					state.apply(diff);
				}
			}
		} catch (RuntimeException e) {
			invalidateInternalState();
			throw e;
		}
	}

	private void apply(List<D> diffs) {
		try {
			for (D op : diffs) {
				state.apply(op);
			}
		} catch (RuntimeException e) {
			invalidateInternalState();
			throw e;
		}
	}

	private void invalidateInternalState() {
		head = null;
		workingDiffs = null;
		pendingCommits = null;
		state = null;
	}

	private boolean isInternalStateValid() {
		return head != null;
	}

	public CompletionStage<Void> makeCheckpoint() {
		return makeCheckpointForNode(head).thenCompose(k -> pull());
	}

// Helper OT methods

	public CompletionStage<FindResult<K, D>> findCheckpoint() {
		return OTUtils.findCheckpoint(source, comparator);
	}

	public CompletionStage<FindResult<K, D>> findCheckpoint(Set<K> startNodes, @Nullable K lastNode) {
		return OTUtils.findCheckpoint(source, comparator, startNodes, lastNode);
	}

	public CompletionStage<FindResult<K, D>> findParent(K startNode, @Nullable K lastNode,
	                                                    Predicate<OTCommit<K, D>> matchPredicate) {
		return OTUtils.findParent(source, comparator, startNode, lastNode, matchPredicate);
	}

	public CompletionStage<FindResult<K, D>> findParent(Set<K> startNodes, @Nullable K lastNode,
	                                                    Predicate<OTCommit<K, D>> matchPredicate) {
		return OTUtils.findParent(source, comparator, startNodes, lastNode, matchPredicate);
	}

	public CompletionStage<K> makeCheckpointForHeads() {
		return OTUtils.makeCheckpointForHeads(otSystem, source, comparator);
	}

	public CompletionStage<K> makeCheckpointForNode(K node) {
		return OTUtils.makeCheckpointForNode(otSystem, source, comparator, node);
	}

	public CompletionStage<K> mergeHeadsAndPush() {
		return OTUtils.mergeHeadsAndPush(otSystem, source, comparator);
	}

//	public void merge( Set<K> nodes,
//	                   ResultCallback<OTCommit<K, D>> callback) {
//		OTUtils.merge(eventloop, otSystem, source, comparator, nodes, callback);
//	}

	@Override
	public String toString() {
		return "{" +
				"state=" + state +
				", head=" + head +
				", workingDiffs=" + workingDiffs +
				", pendingCommits=" + pendingCommits +
				'}';
	}
}
