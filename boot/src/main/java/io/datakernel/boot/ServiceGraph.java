/*
 * Copyright (C) 2015 SoftIndex LLC.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.datakernel.boot;

import com.google.common.collect.LinkedHashMultimap;
import com.google.common.collect.Multimap;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import io.datakernel.annotation.Nullable;
import io.datakernel.util.Stopwatch;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static com.google.common.base.Strings.repeat;
import static com.google.common.base.Throwables.getRootCause;
import static com.google.common.collect.Iterables.concat;
import static com.google.common.collect.Sets.difference;
import static com.google.common.collect.Sets.union;
import static io.datakernel.util.Preconditions.checkArgument;
import static io.datakernel.util.Preconditions.checkState;
import static java.util.Arrays.asList;
import static java.util.Collections.singleton;
import static java.util.concurrent.Executors.newSingleThreadExecutor;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

public final class ServiceGraph {

	private static final Logger logger = LoggerFactory.getLogger(ServiceGraph.class);

	/**
	 * This set used to represent edges between vertices. If N1 and N2 - nodes and between them
	 * exists edge from N1 to N2, it can be represent as adding to this SetMultimap element <N1,N2>.
	 * This collection consist of nodes in which there are edges and their keys - previous nodes.
	 */
	private final Multimap<Object, Object> forwards = LinkedHashMultimap.create();

	/**
	 * This set used to represent edges between vertices. If N1 and N2 - nodes and between them
	 * exists edge from N1 to N2, it can be represent as adding to this SetMultimap element <N2,N1>
	 * This collection consist of nodes in which there are edges and their keys - previous nodes.
	 */
	private final Multimap<Object, Object> backwards = LinkedHashMultimap.create();

	private final Set<Object> runningNodes = new HashSet<>();

	private final Map<Object, Service> services = new HashMap<>();

	private ServiceGraph() {
	}

	public static ServiceGraph create() {
		return new ServiceGraph();
	}

	public ServiceGraph add(Object key, Service service, Object... dependencies) {
		checkArgument(!services.containsKey(key));
		if (service != null) {
			services.put(key, service);
		}
		add(key, asList(dependencies));
		return this;
	}

	public final ServiceGraph add(Object key, Object first, Object... rest) {
		add(key, concat(singleton(first), asList(rest)));
		return this;
	}

	public final ServiceGraph add(Object key, Iterable<Object> dependencies) {
		for (Object dependency : dependencies) {
			checkArgument(!(dependency instanceof Service), "Dependency %s must be a key, not a service", dependency);
			forwards.put(key, dependency);
			backwards.put(dependency, key);
		}
		return this;
	}

	private ListenableFuture<LongestPath> processNode(final Object node, final boolean start,
	                                                  Map<Object, ListenableFuture<LongestPath>> futures, final Executor executor) {
		List<ListenableFuture<LongestPath>> dependencyFutures = new ArrayList<>();
		for (Object dependencyNode : (start ? forwards : backwards).get(node)) {
			ListenableFuture<LongestPath> dependencyFuture = processNode(dependencyNode, start, futures, executor);
			dependencyFutures.add(dependencyFuture);
		}

		if (futures.containsKey(node)) {
			return futures.get(node);
		}

		final SettableFuture<LongestPath> future = SettableFuture.create();
		futures.put(node, future);

		final ListenableFuture<LongestPath> dependenciesFuture = combineDependenciesFutures(dependencyFutures, executor);

		dependenciesFuture.addListener(new Runnable() {
			@Override
			public void run() {
				try {
					final LongestPath longestPath = dependenciesFuture.get();

					Service service = services.get(node);
					if (service == null) {
						logger.debug("...skipping no-service node: " + nodeToString(node));
						future.set(longestPath);
						return;
					}

					if (!start && !runningNodes.contains(node)) {
						logger.debug("...skipping not running node: " + nodeToString(node));
						future.set(longestPath);
						return;
					}

					final Stopwatch sw = Stopwatch.createStarted();
					final ListenableFuture<?> serviceFuture = (start ? service.start() : service.stop());
					logger.info((start ? "Starting" : "Stopping") + " node: " + nodeToString(node));
					serviceFuture.addListener(new Runnable() {
						@Override
						public void run() {
							try {
								serviceFuture.get();

								if (start) {
									runningNodes.add(node);
								} else {
									runningNodes.remove(node);
								}

								long elapsed = sw.elapsed(MILLISECONDS);
								logger.info((start ? "Started" : "Stopped") + " " + nodeToString(node) + (elapsed >= 1L ? (" in " + sw) : ""));
								future.set(new LongestPath(elapsed + (longestPath != null ? longestPath.totalTime : 0),
										elapsed, node, longestPath));
							} catch (InterruptedException | ExecutionException e) {
								logger.error("error: " + nodeToString(node), e);
								future.setException(getRootCause(e));
							}
						}
					}, executor);
				} catch (InterruptedException | ExecutionException e) {
					future.setException(getRootCause(e));
				}
			}
		}, executor);

		return future;
	}

	private ListenableFuture<LongestPath> combineDependenciesFutures(List<ListenableFuture<LongestPath>> futures, Executor executor) {
		if (futures.size() == 0) {
			return Futures.immediateFuture(null);
		}
		if (futures.size() == 1) {
			return futures.get(0);
		}

		final SettableFuture<LongestPath> settableFuture = SettableFuture.create();
		final AtomicInteger atomicInteger = new AtomicInteger(futures.size());
		final AtomicReference<LongestPath> bestPath = new AtomicReference<>();
		final AtomicReference<Throwable> exception = new AtomicReference<>();
		for (final ListenableFuture<LongestPath> future : futures) {
			future.addListener(new Runnable() {
				@Override
				public void run() {
					try {
						LongestPath path = future.get();
						if (bestPath.get() == null || (path != null && path.totalTime > bestPath.get().totalTime)) {
							bestPath.set(path);
						}
					} catch (InterruptedException | ExecutionException e) {
						if (exception.get() == null) {
							exception.set(getRootCause(e));
						}
					}
					if (atomicInteger.decrementAndGet() == 0) {
						if (exception.get() != null) {
							settableFuture.setException(exception.get());
						} else {
							settableFuture.set(bestPath.get());
						}
					}
				}
			}, executor);
		}
		return settableFuture;
	}

	/**
	 * Stops services from  the service graph
	 */
	synchronized public ListenableFuture<?> startFuture() {
		List<Object> circularDependencies = findCircularDependencies();
		checkState(circularDependencies == null, "Circular dependencies found: %s", circularDependencies);
		Set<Object> rootNodes = difference(union(services.keySet(), forwards.keySet()), backwards.keySet());
		logger.info("Starting services");
		logger.debug("Root nodes: {}", rootNodes);
		return actionInThread(true, rootNodes);
	}

	/**
	 * Stops services from  the service graph
	 */
	synchronized public ListenableFuture<?> stopFuture() {
		Set<Object> leafNodes = difference(union(services.keySet(), backwards.keySet()), forwards.keySet());
		logger.info("Stopping services");
		logger.debug("Leaf nodes: {}", leafNodes);
		return actionInThread(false, leafNodes);
	}

	private ListenableFuture<?> actionInThread(final boolean start, final Collection<Object> rootNodes) {
		final SettableFuture<?> resultFuture = SettableFuture.create();
		final ExecutorService executor = newSingleThreadExecutor();
		executor.execute(new Runnable() {
			@Override
			public void run() {
				Map<Object, ListenableFuture<LongestPath>> futures = new HashMap<>();
				List<ListenableFuture<LongestPath>> rootFutures = new ArrayList<>();
				for (Object rootNode : rootNodes) {
					rootFutures.add(processNode(rootNode, start, futures, executor));
				}
				final ListenableFuture<LongestPath> rootFuture = combineDependenciesFutures(rootFutures, executor);
				rootFuture.addListener(new Runnable() {
					@Override
					public void run() {
						try {
							LongestPath longestPath = rootFuture.get();
							StringBuilder sb = new StringBuilder();
							printLongestPath(sb, longestPath);
							logger.info("Longest path:\n" + sb);
							resultFuture.set(null);
							executor.shutdown();
						} catch (InterruptedException | ExecutionException e) {
							resultFuture.setException(getRootCause(e));
							executor.shutdown();
						}
					}
				}, executor);
			}
		});
		return resultFuture;
	}

	private void printLongestPath(StringBuilder sb, LongestPath longestPath) {
		if (longestPath == null)
			return;
		printLongestPath(sb, longestPath.tail);
		sb.append(nodeToString(longestPath.head)).append(" : ");
		sb.append(String.format("%1.3f sec", longestPath.time / 1000.0));
		sb.append("\n");
	}

	@Override
	@SuppressWarnings("StringConcatenationInsideStringBufferAppend")
	public String toString() {
		StringBuilder sb = new StringBuilder();
		Set<Object> visited = new LinkedHashSet<>();
		List<Iterator<Object>> path = new ArrayList<>();
		Iterable<Object> roots = difference(union(services.keySet(), forwards.keySet()), backwards.keySet());
		path.add(roots.iterator());
		while (!path.isEmpty()) {
			Iterator<Object> it = path.get(path.size() - 1);
			if (it.hasNext()) {
				Object node = it.next();
				if (!visited.contains(node)) {
					visited.add(node);
					sb.append(repeat("\t", path.size() - 1) + "" + nodeToString(node) + "\n");
					path.add(forwards.get(node).iterator());
				} else {
					sb.append(repeat("\t", path.size() - 1) + nodeToString(node) + " ^" + "\n");
				}
			} else {
				path.remove(path.size() - 1);
			}
		}
		return sb.toString();
	}

	private void removeIntermediate(Object vertex) {
		for (Object backward : backwards.get(vertex)) {
			forwards.get(backward).remove(vertex);
			for (Object forward : forwards.get(vertex)) {
				if (!forward.equals(backward)) {
					forwards.get(backward).add(forward);
				}
			}
		}
		for (Object forward : forwards.get(vertex)) {
			backwards.get(forward).remove(vertex);
			for (Object backward : backwards.get(vertex)) {
				if (!forward.equals(backward)) {
					backwards.get(forward).add(backward);
				}
			}
		}

		forwards.removeAll(vertex);
		backwards.removeAll(vertex);
	}

	/**
	 * Removes nodes which don't have services
	 */
	public void removeIntermediateNodes() {
		List<Object> toRemove = new ArrayList<>();
		for (Object v : union(forwards.keySet(), backwards.keySet())) {
			if (!services.containsKey(v)) {
				toRemove.add(v);
			}
		}

		for (Object v : toRemove) {
			removeIntermediate(v);
		}
	}

	private List<Object> findCircularDependencies() {
		Set<Object> visited = new LinkedHashSet<>();
		List<Object> path = new ArrayList<>();
		next:
		while (true) {
			for (Object node : path.isEmpty() ? services.keySet() : forwards.get(path.get(path.size() - 1))) {
				int loopIndex = path.indexOf(node);
				if (loopIndex != -1) {
					logger.warn("Circular dependencies found: " + nodesToString(path.subList(loopIndex, path.size())));
					return path.subList(loopIndex, path.size());
				}
				if (!visited.contains(node)) {
					visited.add(node);
					path.add(node);
					continue next;
				}
			}
			if (path.isEmpty())
				break;
			path.remove(path.size() - 1);
		}
		return null;
	}

	private String nodeToString(Object node) {
		return node.toString();
	}

	private String nodesToString(Iterable<Object> newNodes) {
		StringBuilder sb = new StringBuilder().append("[");
		Iterator<Object> iterator = newNodes.iterator();

		while (iterator.hasNext()) {
			sb.append(nodeToString(iterator.next()));
			if (iterator.hasNext()) {
				sb.append(", ");
			}
		}
		return sb.append("]").toString();
	}

	public interface Service {
		ListenableFuture<?> start();

		ListenableFuture<?> stop();
	}

	private static class LongestPath {
		private final long totalTime;
		private final long time;
		private final Object head;
		@Nullable
		private final LongestPath tail;

		private LongestPath(long totalTime, long time, Object head, LongestPath tail) {
			this.totalTime = totalTime;
			this.time = time;
			this.head = head;
			this.tail = tail;
		}
	}

}