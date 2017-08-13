package io.datakernel.ot;

import com.google.common.base.Predicate;
import com.google.common.base.Predicates;
import io.datakernel.annotation.Nullable;
import io.datakernel.async.ForwardingResultCallback;
import io.datakernel.async.ResultCallback;
import io.datakernel.eventloop.Eventloop;

import java.util.*;

import static io.datakernel.async.AsyncCallbacks.postTo;
import static io.datakernel.async.AsyncCallbacks.toResultCallback;
import static io.datakernel.util.Preconditions.checkNotNull;
import static java.util.Collections.singleton;
import static java.util.Collections.singletonList;

public class OTUtils {
	private OTUtils() {
	}

	public static final class FindResult<K, D> {
		@Nullable
		private final OTCommit<K, D> parentCommit;
		@Nullable
		private final K child;
		@Nullable
		private final List<D> parentToChild;

		private FindResult(OTCommit<K, D> parentCommit, K child, List<D> parentToChild) {
			this.child = child;
			this.parentCommit = parentCommit;
			this.parentToChild = parentToChild;
		}

		public static <K, D> FindResult<K, D> of(OTCommit<K, D> parent, K child, List<D> pathToParent) {
			return new FindResult<>(parent, child, pathToParent);
		}

		public static <K, D> FindResult<K, D> notFound() {
			return new FindResult<>(null, null, null);
		}

		boolean isFound() {
			return parentCommit != null;
		}

		public OTCommit<K, D> getParentCommit() {
			return checkNotNull(parentCommit);
		}

		public K getChild() {
			return checkNotNull(child);
		}

		public List<D> getParentToChild() {
			return checkNotNull(parentToChild);
		}

		@Override
		public String toString() {
			return "FindResult{" +
					"parentCommit=" + parentCommit +
					", child=" + child +
					", parentToChild=" + parentToChild +
					'}';
		}
	}

	private static final class Entry<K, D> {
		final K parent;
		final K child;
		final List<D> parentToChild;

		private Entry(K parent, K child, List<D> parentToChild) {
			this.parent = parent;
			this.child = child;
			this.parentToChild = parentToChild;
		}

		@Override
		public String toString() {
			return "Entry{" +
					"parent=" + parent +
					", child=" + child +
					", parentToChild=" + parentToChild +
					'}';
		}
	}

	public static <K, D> void findCheckpoint(final Eventloop eventloop, final OTRemote<K, D> source, final Comparator<K> keyComparator,
	                                         final ResultCallback<FindResult<K, D>> callback) {
		source.getHeads(new ForwardingResultCallback<Set<K>>(callback) {
			@Override
			protected void onResult(Set<K> heads) {
				findCheckpoint(eventloop, source, keyComparator, heads, null, new ForwardingResultCallback<OTUtils.FindResult<K, D>>(callback) {
					@Override
					protected void onResult(OTUtils.FindResult<K, D> result) {
						if (result.isFound()) {
							callback.setResult(result);
						} else {
							callback.setException(new IllegalStateException("Could not find snapshot"));
						}
					}
				});
			}
		});
	}

	public static <K, D> void findCheckpoint(Eventloop eventloop, OTRemote<K, D> source, Comparator<K> keyComparator,
	                                         Set<K> startNodes, @Nullable K lastNode,
	                                         ResultCallback<FindResult<K, D>> callback) {
		findParent(eventloop, source, keyComparator,
				startNodes, lastNode,
				new Predicate<OTCommit<K, D>>() {
					@Override
					public boolean apply(OTCommit<K, D> input) {
						return input.isCheckpoint();
					}
				},
				callback);
	}

	public static <K, D> void findParent(final Eventloop eventloop, final OTRemote<K, D> source, final Comparator<K> keyComparator,
	                                     K startNode, @Nullable final K lastNode,
	                                     Predicate<OTCommit<K, D>> matchPredicate,
	                                     ResultCallback<FindResult<K, D>> callback) {
		findParent(eventloop, source, keyComparator, singleton(startNode), lastNode, matchPredicate, callback);
	}

	public static <K, D> void findParent(final Eventloop eventloop, final OTRemote<K, D> source, final Comparator<K> keyComparator,
	                                     Set<K> startNodes, @Nullable final K lastNode,
	                                     Predicate<OTCommit<K, D>> matchPredicate,
	                                     ResultCallback<FindResult<K, D>> callback) {
		findParent(eventloop, source, keyComparator,
				startNodes,
				lastNode == null ?
						Predicates.<K>alwaysTrue() :
						new Predicate<K>() {
							@Override
							public boolean apply(K key) {
								return keyComparator.compare(key, lastNode) >= 0;
							}
						},
				matchPredicate,
				callback);
	}

	public static <K, D> void findParent(final Eventloop eventloop, final OTRemote<K, D> source, final Comparator<K> keyComparator,
	                                     Set<K> startNodes, Predicate<K> loadPredicate,
	                                     Predicate<OTCommit<K, D>> matchPredicate,
	                                     ResultCallback<FindResult<K, D>> callback) {
		PriorityQueue<Entry<K, D>> queue = new PriorityQueue<>(11,
				new Comparator<Entry<K, D>>() {
					@Override
					public int compare(Entry<K, D> o1, Entry<K, D> o2) {
						return keyComparator.compare(o2.parent, o1.parent);
					}
				});
		for (K startNode : startNodes) {
			queue.add(new Entry<>(startNode, startNode, Collections.<D>emptyList()));
		}
		findParent(eventloop, source, queue, new HashSet<K>(), loadPredicate, matchPredicate, callback);
	}

	private static <K, D> void findParent(final Eventloop eventloop, final OTRemote<K, D> source,
	                                      final PriorityQueue<Entry<K, D>> queue, final Set<K> visited,
	                                      final Predicate<K> loadPredicate,
	                                      final Predicate<OTCommit<K, D>> matchPredicate,
	                                      final ResultCallback<FindResult<K, D>> callback) {
		while (!queue.isEmpty()) {
			final Entry<K, D> nodeWithPath = queue.poll();
			final K node = nodeWithPath.parent;
			if (!visited.add(node))
				continue;
			source.loadCommit(node, new ForwardingResultCallback<OTCommit<K, D>>(callback) {
				@Override
				protected void onResult(OTCommit<K, D> commit) {
					if (matchPredicate.apply(commit)) {
						List<D> path = new ArrayList<>();
						path.addAll(nodeWithPath.parentToChild);
						callback.setResult(FindResult.of(commit, nodeWithPath.child, path));
						return;
					}
					for (Map.Entry<K, List<D>> parentEntry : commit.getParents().entrySet()) {
						K parent = parentEntry.getKey();
						if (parentEntry.getValue() == null)
							continue;
						if (loadPredicate.apply(parent)) {
							List<D> parentDiffs = new ArrayList<>();
							parentDiffs.addAll(parentEntry.getValue());
							parentDiffs.addAll(nodeWithPath.parentToChild);
							queue.add(new Entry<>(parent, nodeWithPath.child, parentDiffs));
						}
					}
					eventloop.post(new Runnable() {
						@Override
						public void run() {
							findParent(eventloop, source, queue, visited, loadPredicate, matchPredicate, callback);
						}
					});
				}
			});
			return;
		}
		callback.setResult(FindResult.<K, D>notFound());
	}

	public static <K1, K2, V> Map<K2, V> ensureMapValue(Map<K1, Map<K2, V>> map, K1 key) {
		Map<K2, V> value = map.get(key);
		if (value == null) {
			value = new HashMap<>();
			map.put(key, value);
		}
		return value;
	}

	public static <K, V> Set<V> ensureSetValue(Map<K, Set<V>> map, K key) {
		Set<V> value = map.get(key);
		if (value == null) {
			value = new HashSet<>();
			map.put(key, value);
		}
		return value;
	}

	public static <K, V> List<V> ensureListValue(Map<K, List<V>> map, K key) {
		List<V> value = map.get(key);
		if (value == null) {
			value = new ArrayList<>();
			map.put(key, value);
		}
		return value;
	}

	private static <K, D> void loadCommitForMerge(final Eventloop eventloop, final OTRemote<K, D> source, final OTSystem<D> otSystem,
	                                              final K node, final Set<K> visitedNodes,
	                                              final List<D> squashedPath, final Set<K> squashedPathNodes,
	                                              final ResultCallback<OTCommit<K, D>> callback) {
		squashedPathNodes.add(node);
		source.loadCommit(node, postTo(eventloop, new ForwardingResultCallback<OTCommit<K, D>>(callback) {
			@Override
			protected void onResult(OTCommit<K, D> commit) {
				if (commit.isRoot() || commit.isMerge() || visitedNodes.contains(commit.getId())) {
					callback.setResult(commit);
					return;
				}
				assert commit.getParents().size() == 1;
				Map.Entry<K, List<D>> parentEntry = commit.getParents().entrySet().iterator().next();
				K parentId = parentEntry.getKey();
				List<D> parentPath = parentEntry.getValue();
				squashedPath.addAll(0, parentPath);
				loadCommitForMerge(eventloop, source, otSystem, parentId, visitedNodes, squashedPath, squashedPathNodes, callback);
			}
		}));
	}

	public static <K, D> void merge(final Eventloop eventloop, final OTSystem<D> otSystem, final OTRemote<K, D> source, final Comparator<K> keyComparator,
	                                final Set<K> nodes,
	                                final ResultCallback<Map<K, List<D>>> callback) {
		doMerge(eventloop, otSystem, source, keyComparator, nodes, new HashSet<K>(), null, callback);
	}

	@SuppressWarnings("unchecked")
	public static <K, D> void doMerge(final Eventloop eventloop, final OTSystem<D> otSystem, final OTRemote<K, D> source, final Comparator<K> keyComparator,
	                                  final Set<K> nodes, final Set<K> visitedNodes, final K rootNode,
	                                  final ResultCallback<Map<K, List<D>>> callback) {
		if (nodes.size() == 0) {
			callback.setResult(Collections.<K, List<D>>emptyMap());
			return;
		}

		if (nodes.size() == 1) {
			Map<K, List<D>> result = new HashMap<>();
			K node = nodes.iterator().next();
			result.put(node, Collections.<D>emptyList());
			visitedNodes.add(node);
			callback.setResult(result);
			return;
		}

		K pivot = null;
		for (K node : nodes) {
			if (rootNode != null && rootNode.equals(node)) {
				continue;
			}
			if (pivot == null || keyComparator.compare(pivot, node) > 0) {
				pivot = node;
			}
		}
		final HashSet<K> otherNodes = new HashSet<>(nodes);
		otherNodes.remove(pivot);

		final List<D> squashPath = new ArrayList<>();
		final Set<K> squashNodes = new HashSet<>();
		final K finalPivot = pivot;
		loadCommitForMerge(eventloop, source, otSystem, pivot, visitedNodes, squashPath, squashNodes, postTo(eventloop, new ForwardingResultCallback<OTCommit<K, D>>(callback) {
			@Override
			protected void onResult(final OTCommit<K, D> commit) {
				if (rootNode == null && commit.isRoot()) {
					doMerge(eventloop, otSystem, source, keyComparator, nodes, visitedNodes, commit.getId(), callback);
					return;
				}
				visitedNodes.addAll(squashNodes);

				Set<K> recursiveMergeNodes = new HashSet<>(otherNodes);
				recursiveMergeNodes.addAll(commit.isRoot() ? singleton(commit.getId()) : commit.getParents().keySet());

				doMerge(eventloop, otSystem, source, keyComparator, recursiveMergeNodes, visitedNodes, rootNode, postTo(eventloop, new ForwardingResultCallback<Map<K, List<D>>>(callback) {
					@Override
					protected void onResult(Map<K, List<D>> mergeResult) {
						K parent = null;
						for (Map.Entry<K, List<D>> entry : commit.getParents().entrySet()) {
							if (entry.getValue() == null)
								continue;
							parent = entry.getKey();
							break;
						}

						if (commit.isRoot())
							parent = commit.getId();

						Map<K, List<D>> result = new HashMap<>();

						List<D> pivotPath = new ArrayList<>();
						if (!commit.isRoot())
							pivotPath.addAll(otSystem.invert(commit.getParents().get(parent)));
						pivotPath.addAll(mergeResult.get(parent));

						DiffPair<D> transformed = otSystem.transform(DiffPair.of(otSystem.squash(squashPath), otSystem.squash(pivotPath)));

						result.put(finalPivot, transformed.left);

						for (K node : otherNodes) {
							List<D> list = new ArrayList<>();
							list.addAll(mergeResult.get(node));
							list.addAll(transformed.right);
							result.put(node, list);
						}

						callback.setResult(result);
					}
				}));
			}
		}));
	}

	public static <K, D> void mergeHeadsAndPush(final Eventloop eventloop, final OTSystem<D> otSystem, final OTRemote<K, D> source, final Comparator<K> keyComparator,
	                                            final ResultCallback<K> callback) {
		source.getHeads(new ForwardingResultCallback<Set<K>>(callback) {
			@Override
			protected void onResult(Set<K> heads) {
				merge(eventloop, otSystem, source, keyComparator, heads, new ForwardingResultCallback<Map<K, List<D>>>(callback) {
					@Override
					protected void onResult(final Map<K, List<D>> merge) {
						source.createId(new ForwardingResultCallback<K>(callback) {
							@Override
							protected void onResult(K mergeCommitId) {
								source.push(singletonList(OTCommit.ofMerge(mergeCommitId, merge)),
										toResultCallback(callback, mergeCommitId));
							}
						});
					}
				});
			}
		});
	}

	private static <K, D> void doMakeCheckpoint(final OTSystem<D> otSystem, final OTRemote<K, D> source,
	                                            final FindResult<K, D> result,
	                                            final ResultCallback<K> callback) {
		if (result.isFound()) {
			List<D> diffs = new ArrayList<>();
			diffs.addAll(result.getParentCommit().getCheckpoint());
			diffs.addAll(result.getParentToChild());
			final List<D> checkpoint = otSystem.squash(diffs);

			source.createId(new ForwardingResultCallback<K>(callback) {
				@Override
				protected void onResult(final K checkpointId) {
					OTCommit<K, D> commit = OTCommit.ofCheckpoint(checkpointId, result.getChild(), checkpoint);
					source.push(singletonList(commit),
							toResultCallback(callback, checkpointId));
				}
			});
		} else {
			callback.setException(new IllegalArgumentException("No checkpoint found for HEAD(s)"));
		}
	}

	public static <K, D> void makeCheckpointForHeads(final Eventloop eventloop, final OTSystem<D> otSystem, final OTRemote<K, D> source, final Comparator<K> keyComparator,
	                                                 final ResultCallback<K> callback) {
		findCheckpoint(eventloop, source, keyComparator, new ForwardingResultCallback<FindResult<K, D>>(callback) {
			@Override
			protected void onResult(final FindResult<K, D> result) {
				doMakeCheckpoint(otSystem, source, result, callback);
			}
		});
	}

	public static <K, D> void makeCheckpointForNode(final Eventloop eventloop, final OTSystem<D> otSystem, final OTRemote<K, D> source, final Comparator<K> keyComparator,
	                                                K node,
	                                                final ResultCallback<K> callback) {
		findCheckpoint(eventloop, source, keyComparator, singleton(node), null, new ForwardingResultCallback<FindResult<K, D>>(callback) {
			@Override
			protected void onResult(final FindResult<K, D> result) {
				doMakeCheckpoint(otSystem, source, result, callback);
			}
		});
	}

}
