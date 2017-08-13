package io.datakernel.ot;

import com.google.common.base.Predicate;
import io.datakernel.annotation.Nullable;
import io.datakernel.async.CompletionCallback;
import io.datakernel.async.ForwardingCompletionCallback;
import io.datakernel.async.ForwardingResultCallback;
import io.datakernel.async.ResultCallback;
import io.datakernel.eventloop.Eventloop;
import io.datakernel.eventloop.EventloopService;
import io.datakernel.ot.OTUtils.FindResult;

import java.util.*;

import static java.util.Collections.singleton;

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
	public void start(final CompletionCallback callback) {
		checkout(new ForwardingCompletionCallback(callback) {
			@Override
			public void onComplete() {
				rebase(callback);
			}
		});
	}

	@Override
	public void stop(CompletionCallback callback) {
		invalidateInternalState();
	}

	public void checkout(final CompletionCallback callback) {
		head = null;
		workingDiffs.clear();
		pendingCommits.clear();
		state.init();
		source.getCheckpoint(new ForwardingResultCallback<K>(callback) {
			@Override
			public void onResult(final K checkpointId) {
				source.loadCommit(checkpointId, new ForwardingResultCallback<OTCommit<K, D>>(callback) {
					@Override
					public void onResult(final OTCommit<K, D> commit) {
						head = checkpointId;
						doApply(commit.getCheckpoint());
						callback.setComplete();
					}
				});
			}
		});
	}

	public void checkout(K commitId, final CompletionCallback callback) {
		head = null;
		workingDiffs.clear();
		pendingCommits.clear();
		state.init();
		findCheckpoint(singleton(commitId), null, new ForwardingResultCallback<FindResult<K, D>>(callback) {
			@Override
			public void onResult(final FindResult<K, D> findResult) {
				if (!findResult.isFound()) {
					callback.setException(new IllegalStateException("Could not find path to original head"));
					return;
				}
				final List<D> pathToNewHead = findResult.getParentToChild();
				doApply(findResult.getParentCommit().getCheckpoint());
				doApply(pathToNewHead);
				head = findResult.getChild();
				callback.setComplete();
			}
		});
	}

	public void rebase(final CompletionCallback callback) {
		if (!pendingCommits.isEmpty()) {
			callback.setComplete();
			return;
		}
		source.getHeads(new ForwardingResultCallback<Set<K>>(callback) {
			@Override
			public void onResult(final Set<K> newHeads) {
				final Predicate<OTCommit<K, D>> isHead = new Predicate<OTCommit<K, D>>() {
					@Override
					public boolean apply(final OTCommit<K, D> input) {
						return input.getId() == head;
					}
				};
				findParent(newHeads, head, isHead, new ForwardingResultCallback<FindResult<K, D>>(callback) {
					@Override
					public void onResult(final FindResult<K, D> findResult) {
						if (!findResult.isFound()) {
							callback.setException(new IllegalStateException("Could not find path to original head"));
							return;
						}
						if (!pendingCommits.isEmpty()) {
							callback.setComplete();
							return;
						}
						final List<D> pathToNewHead = findResult.getParentToChild();
						final DiffPair<D> transformed = otSystem.transform(DiffPair.of(otSystem.squash(workingDiffs), otSystem.squash(pathToNewHead)));
						doApply(transformed.left);
						workingDiffs = new ArrayList<>(transformed.right);
						head = findResult.getChild();
						callback.setComplete();
					}
				});
			}
		});
	}

	public void reset() {
		List<D> diffs = new ArrayList<>();
		for (List<D> ds : pendingCommits.values()) {
			diffs.addAll(ds);
		}
		diffs.addAll(workingDiffs);
		diffs = otSystem.invert(diffs);
		doApply(diffs);
		pendingCommits = new LinkedHashMap<>();
		workingDiffs = new ArrayList<>();
	}

	public void commitAndPush(final CompletionCallback callback) {
		commit(new ForwardingCompletionCallback(callback) {
			@Override
			public void onComplete() {
				push(callback);
			}
		});
	}

	public void rebaseAndCommitAndPush(final CompletionCallback callback) {
		rebase(new ForwardingCompletionCallback(callback) {
			@Override
			public void onComplete() {
				commit(new ForwardingCompletionCallback(callback) {
					@Override
					public void onComplete() {
						push(callback);
					}
				});
			}
		});
	}

	public void commit(final CompletionCallback callback) {
		if (workingDiffs.isEmpty()) {
			callback.setComplete();
			return;
		}
		source.createId(new ForwardingResultCallback<K>(callback) {
			@Override
			public void onResult(final K newId) {
				pendingCommits.put(newId, otSystem.squash(workingDiffs));
				workingDiffs = new ArrayList<>();
				callback.setComplete();
			}
		});
	}

	public void push(final CompletionCallback callback) {
		if (pendingCommits.isEmpty()) {
			callback.setComplete();
			return;
		}
		K parent = head;
		final List<OTCommit<K, D>> list = new ArrayList<>();
		for (Map.Entry<K, List<D>> pendingCommitEntry : pendingCommits.entrySet()) {
			K key = pendingCommitEntry.getKey();
			List<D> diffs = pendingCommitEntry.getValue();
			OTCommit<K, D> commit = OTCommit.ofCommit(key, parent, diffs);
			list.add(commit);
			parent = key;
		}
		source.push(list, new ForwardingCompletionCallback(callback) {
			@Override
			public void onComplete() {
				pendingCommits = new LinkedHashMap<>();
				head = list.get(list.size() - 1).getId();
				callback.setComplete();
			}
		});
	}

	public void apply(D diff) {
		applyDiffs(Collections.singletonList(diff));
	}

	public void applyDiffs(List<D> diffs) {
		for (D diff : diffs) {
			if (!otSystem.isEmpty(diff)) {
				workingDiffs.addAll(diffs);
			}
		}
		doApply(diffs);
	}

	private void doApply(List<D> diffs) {
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

	public void makeCheckpoint(final CompletionCallback callback) {
		makeCheckpointForNode(head, new ForwardingResultCallback<K>(callback) {
			@Override
			public void onResult(K value) {
				rebase(callback);
			}
		});
	}

	// Helper OT methods

	public void findCheckpoint(final ResultCallback<FindResult<K, D>> callback) {
		OTUtils.findCheckpoint(eventloop, source, comparator, callback);
	}

	public void findCheckpoint(Set<K> startNodes, @Nullable K lastNode,
	                           ResultCallback<FindResult<K, D>> callback) {
		OTUtils.findCheckpoint(eventloop, source, comparator, startNodes, lastNode, callback);
	}

	public void findParent(K startNode, @Nullable final K lastNode,
	                       Predicate<OTCommit<K, D>> matchPredicate,
	                       ResultCallback<FindResult<K, D>> callback) {
		OTUtils.findParent(eventloop, source, comparator, startNode, lastNode, matchPredicate, callback);
	}

	public void findParent(Set<K> startNodes, @Nullable final K lastNode,
	                       Predicate<OTCommit<K, D>> matchPredicate,
	                       ResultCallback<FindResult<K, D>> callback) {
		OTUtils.findParent(eventloop, source, comparator, startNodes, lastNode, matchPredicate, callback);
	}

	public void makeCheckpointForHeads(final ResultCallback<K> callback) {
		OTUtils.makeCheckpointForHeads(eventloop, otSystem, source, comparator, callback);
	}

	public void makeCheckpointForNode(K node, final ResultCallback<K> callback) {
		OTUtils.makeCheckpointForNode(eventloop, otSystem, source, comparator, node, callback);
	}

	public void mergeHeadsAndPush(final ResultCallback<K> callback) {
		OTUtils.mergeHeadsAndPush(eventloop, otSystem, source, comparator, callback);
	}

//	public void merge(final Set<K> nodes,
//	                  final ResultCallback<OTCommit<K, D>> callback) {
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
