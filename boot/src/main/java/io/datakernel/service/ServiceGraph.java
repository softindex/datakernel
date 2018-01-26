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

package io.datakernel.service;

import com.google.inject.Key;
import io.datakernel.annotation.Nullable;
import io.datakernel.util.Stopwatch;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Stream;

import static io.datakernel.util.CollectionUtils.difference;
import static io.datakernel.util.CollectionUtils.union;
import static io.datakernel.util.Preconditions.checkArgument;
import static io.datakernel.util.Preconditions.checkState;
import static java.util.Arrays.asList;
import static java.util.Collections.emptySet;
import static java.util.concurrent.Executors.newSingleThreadExecutor;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

/**
 * Stores the dependency graph of services. Primarily used by
 * {@link ServiceGraphModule}.
 */
public class ServiceGraph {

	private final Logger logger = LoggerFactory.getLogger(this.getClass());

	/**
	 * This set used to represent edges between vertices. If N1 and N2 - nodes
	 * and between them exists edge from N1 to N2, it can be represent as
	 * adding to this SetMultimap element <N1,N2>. This collection consist of
	 * nodes in which there are edges and their keys - previous nodes.
	 */
	private final Map<Key<?>, Set<Key<?>>> forwards = new HashMap<>();

	/**
	 * This set used to represent edges between vertices. If N1 and N2 - nodes
	 * and between them exists edge from N1 to N2, it can be represent as
	 * adding to this SetMultimap element <N2,N1>. This collection consist of
	 * nodes in which there are edges and their keys - previous nodes.
	 */
	private final Map<Key<?>, Set<Key<?>>> backwards = new HashMap<>();

	private final Set<Key<?>> runningNodes = new HashSet<>();

	private final Map<Key<?>, Service> services = new HashMap<>();

	protected ServiceGraph() {
	}

	public static ServiceGraph create() {
		return new ServiceGraph();
	}

	private static Throwable getRootCause(Throwable throwable) {
		Throwable cause;
		while ((cause = throwable.getCause()) != null) throwable = cause;
		return throwable;
	}

	public ServiceGraph add(Key<?> key, Service service, Key<?>... dependencies) {
		checkArgument(!services.containsKey(key));
		if (service != null) {
			services.put(key, service);
		}
		add(key, asList(dependencies));
		return this;
	}

	public ServiceGraph add(Key<?> key, Iterable<Key<?>> dependencies) {
		for (Key<?> dependency : dependencies) {
			checkArgument(!(dependency instanceof Service), "Dependency %s must be a key, not a service", dependency);
			forwards.computeIfAbsent(key, o -> new HashSet<>()).add(dependency);
			backwards.computeIfAbsent(dependency, o -> new HashSet<>()).add(key);
		}
		return this;
	}

	public ServiceGraph add(Key<?> key, Key<?> first, Key<?>... rest) {
		if (first instanceof Iterable)
			return add(key, (Iterable) first);

		add(key, Stream.concat(Stream.of(first), Arrays.stream(rest))::iterator);
		return this;
	}

	private CompletableFuture<LongestPath> processNode(Key<?> node, boolean start,
	                                                   Map<Key<?>, CompletableFuture<LongestPath>> futures, Executor executor) {
		List<CompletableFuture<LongestPath>> dependencyFutures = new ArrayList<>();
		for (Key<?> dependencyNode : (start ? forwards : backwards).getOrDefault(node, emptySet())) {
			CompletableFuture<LongestPath> dependencyFuture = processNode(dependencyNode, start, futures, executor);
			dependencyFutures.add(dependencyFuture);
		}

		if (futures.containsKey(node)) {
			return futures.get(node);
		}

		CompletableFuture<LongestPath> future = new CompletableFuture<>();
		futures.put(node, future);

		combineDependenciesFutures(dependencyFutures, executor).whenCompleteAsync((longestPath, combineThrowable) -> {
			if (combineThrowable == null) {
				Service service = services.get(node);
				if (service == null) {
					logger.debug("...skipping no-service node: " + nodeToString(node));
					future.complete(longestPath);
					return;
				}

				if (!start && !runningNodes.contains(node)) {
					logger.debug("...skipping not running node: " + nodeToString(node));
					future.complete(longestPath);
					return;
				}

				Stopwatch sw = Stopwatch.createStarted();
				CompletableFuture<Void> serviceFuture = (start ? service.start() : service.stop());
				logger.info((start ? "Starting" : "Stopping") + " node: " + nodeToString(node));
				serviceFuture.whenCompleteAsync((o, throwable) -> {
					if (throwable == null) {
						if (start) {
							runningNodes.add(node);
						} else {
							runningNodes.remove(node);
						}

						long elapsed = sw.elapsed(MILLISECONDS);
						logger.info((start ? "Started" : "Stopped") + " " + nodeToString(node) + (elapsed >= 1L ? (" in " + sw) : ""));
						future.complete(new LongestPath(elapsed + (longestPath != null ? longestPath.totalTime : 0),
								elapsed, node, longestPath));
					} else {
						logger.error("error: " + nodeToString(node), throwable);
						future.completeExceptionally(getRootCause(throwable));
					}
				}, executor);
			} else {
				future.completeExceptionally(getRootCause(combineThrowable));
			}
		}, executor);

		return future;
	}

	private CompletableFuture<LongestPath> combineDependenciesFutures(List<CompletableFuture<LongestPath>> futures, Executor executor) {
		if (futures.size() == 0) {
			return CompletableFuture.completedFuture(null);
		}
		if (futures.size() == 1) {
			return futures.get(0);
		}

		CompletableFuture<LongestPath> settableFuture = new CompletableFuture<>();
		AtomicInteger atomicInteger = new AtomicInteger(futures.size());
		AtomicReference<LongestPath> bestPath = new AtomicReference<>();
		AtomicReference<Throwable> exception = new AtomicReference<>();
		for (CompletableFuture<LongestPath> future : futures) {
			future.whenCompleteAsync((path, throwable) -> {
				if (throwable == null) {
					if (bestPath.get() == null || (path != null && path.totalTime > bestPath.get().totalTime)) {
						bestPath.set(path);
					}
				} else {
					if (exception.get() == null) {
						exception.set(getRootCause(throwable));
					}
				}
				if (atomicInteger.decrementAndGet() == 0) {
					if (exception.get() != null) {
						settableFuture.completeExceptionally(exception.get());
					} else {
						settableFuture.complete(bestPath.get());
					}
				}
			}, executor);
		}
		return settableFuture;
	}

	/**
	 * Stops services from the service graph
	 */
	synchronized public CompletableFuture<?> startFuture() {
		List<Key<?>> circularDependencies = findCircularDependencies();
		checkState(circularDependencies == null, "Circular dependencies found: %s", circularDependencies);
		Set<Key<?>> rootNodes = difference(union(services.keySet(), forwards.keySet()), backwards.keySet());
		logger.info("Starting services");
		logger.debug("Root nodes: {}", rootNodes);
		return actionInThread(true, rootNodes);
	}


	/**
	 * Stops services from the service graph
	 */
	synchronized public CompletableFuture<?> stopFuture() {
		Set<Key<?>> leafNodes = difference(union(services.keySet(), backwards.keySet()), forwards.keySet());
		logger.info("Stopping services");
		logger.debug("Leaf nodes: {}", leafNodes);
		return actionInThread(false, leafNodes);
	}

	private CompletableFuture<?> actionInThread(boolean start, Collection<Key<?>> rootNodes) {
		CompletableFuture<?> resultFuture = new CompletableFuture<>();
		ExecutorService executor = newSingleThreadExecutor();
		executor.execute(() -> {
			Map<Key<?>, CompletableFuture<LongestPath>> futures = new HashMap<>();
			List<CompletableFuture<LongestPath>> rootFutures = new ArrayList<>();
			for (Key<?> rootNode : rootNodes) {
				rootFutures.add(processNode(rootNode, start, futures, executor));
			}
			CompletableFuture<LongestPath> rootFuture = combineDependenciesFutures(rootFutures, executor);
			rootFuture.whenCompleteAsync((longestPath, throwable) -> {
				if (throwable == null) {
					StringBuilder sb = new StringBuilder();
					printLongestPath(sb, longestPath);
					if (sb.length() != 0)
						sb.deleteCharAt(sb.length() - 1);
					logger.info("Longest path:\n" + sb);
					resultFuture.complete(null);
					executor.shutdown();
				} else {
					resultFuture.completeExceptionally(getRootCause(throwable));
					executor.shutdown();
				}
			}, executor);
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
		Set<Key<?>> visited = new LinkedHashSet<>();
		List<Iterator<Key<?>>> path = new ArrayList<>();
		Iterable<Key<?>> roots = difference(union(services.keySet(), forwards.keySet()), backwards.keySet());
		path.add(roots.iterator());
		while (!path.isEmpty()) {
			Iterator<Key<?>> it = path.get(path.size() - 1);
			if (it.hasNext()) {
				Key<?> node = it.next();
				if (!visited.contains(node)) {
					visited.add(node);
					sb.append(repeat("\t", path.size() - 1) + "" + nodeToString(node) + "\n");
					path.add(forwards.getOrDefault(node, emptySet()).iterator());
				} else {
					sb.append(repeat("\t", path.size() - 1) + "" + nodeToString(node) + " *︎" + "\n");
				}
			} else {
				path.remove(path.size() - 1);
			}
		}
		if (sb.length() != 0)
			sb.deleteCharAt(sb.length() - 1);
		return sb.toString();
	}

	private static String repeat(String str, int count) {
		StringBuilder sb = new StringBuilder();
		for (int i = 0; i < count; i++) sb.append(str);
		return sb.toString();
	}

	private static void removeValue(Map<Key<?>, Set<Key<?>>> map, Key<?> key, Key<?> value) {
		Set<Key<?>> objects = map.get(key);
		objects.remove(value);
		if (objects.isEmpty()) map.remove(key);
	}

	private void removeIntermediate(Key<?> vertex) {
		for (Key<?> backward : backwards.getOrDefault(vertex, emptySet())) {
			removeValue(forwards, backward, vertex);
			for (Key<?> forward : forwards.getOrDefault(vertex, emptySet())) {
				if (!forward.equals(backward)) {
					forwards.computeIfAbsent(backward, o -> new HashSet<>()).add(forward);
				}
			}
		}
		for (Key<?> forward : forwards.getOrDefault(vertex, emptySet())) {
			removeValue(backwards, forward, vertex);
			for (Key<?> backward : backwards.getOrDefault(vertex, emptySet())) {
				if (!forward.equals(backward)) {
					backwards.computeIfAbsent(forward, o -> new HashSet<>()).add(backward);
				}
			}
		}

		forwards.remove(vertex);
		backwards.remove(vertex);
	}

	/**
	 * Removes nodes which don't have services
	 */
	public void removeIntermediateNodes() {
		List<Key<?>> toRemove = new ArrayList<>();
		for (Key<?> v : union(forwards.keySet(), backwards.keySet())) {
			if (!services.containsKey(v)) {
				toRemove.add(v);
			}
		}

		for (Key<?> v : toRemove) {
			removeIntermediate(v);
		}
	}

	private List<Key<?>> findCircularDependencies() {
		Set<Key<?>> visited = new LinkedHashSet<>();
		List<Key<?>> path = new ArrayList<>();
		next:
		while (true) {
			for (Key<?> node : path.isEmpty() ? services.keySet() : forwards.getOrDefault(path.get(path.size() - 1), emptySet())) {
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

	protected String nodeToString(Key<?> node) {
		return node.toString();
	}

	private String nodesToString(Iterable<Key<?>> newNodes) {
		StringBuilder sb = new StringBuilder().append("[");
		Iterator<Key<?>> iterator = newNodes.iterator();

		while (iterator.hasNext()) {
			sb.append(nodeToString(iterator.next()));
			if (iterator.hasNext()) {
				sb.append(", ");
			}
		}
		return sb.append("]").toString();
	}

	private static class LongestPath {
		private final long totalTime;
		private final long time;
		private final Key<?> head;
		@Nullable
		private final LongestPath tail;

		private LongestPath(long totalTime, long time, Key<?> head, LongestPath tail) {
			this.totalTime = totalTime;
			this.time = time;
			this.head = head;
			this.tail = tail;
		}
	}

}