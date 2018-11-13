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
import io.datakernel.jmx.ConcurrentJmxMBean;
import io.datakernel.jmx.JmxAttribute;
import io.datakernel.jmx.JmxOperation;
import io.datakernel.util.CollectionUtils;
import io.datakernel.util.Initializable;
import io.datakernel.util.SimpleType;
import io.datakernel.util.Stopwatch;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.annotation.Annotation;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;

import static io.datakernel.util.CollectionUtils.*;
import static io.datakernel.util.Preconditions.checkArgument;
import static io.datakernel.util.Preconditions.checkState;
import static io.datakernel.util.StringFormatUtils.formatDuration;
import static io.datakernel.util.guice.GuiceUtils.prettyPrintAnnotation;
import static java.lang.System.currentTimeMillis;
import static java.util.Arrays.asList;
import static java.util.Collections.emptySet;
import static java.util.Collections.singletonList;
import static java.util.Comparator.comparingLong;
import static java.util.concurrent.Executors.newSingleThreadExecutor;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.stream.Collectors.joining;
import static java.util.stream.Collectors.toList;

/**
 * Stores the dependency graph of services. Primarily used by
 * {@link ServiceGraphModule}.
 */
public final class ServiceGraph implements Initializable<ServiceGraph>, ConcurrentJmxMBean {

	private final Logger logger = LoggerFactory.getLogger(getClass());

	private Runnable startCallback;

	private boolean started;

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

	private final Map<Key<?>, Service> services = new HashMap<>();

	private Function<Key<?>, ?> nodeSuffixes = $ -> "";

	private volatile long startBegin;
	private volatile long startEnd;
	private volatile Throwable startException;
	private volatile SlowestChain slowestChain;

	private volatile long stopBegin;
	private volatile long stopEnd;
	private volatile Throwable stopException;

	private static final class NodeStatus {
		private static final NodeStatus DEFAULT = new NodeStatus();

		volatile long startBegin;
		volatile long startEnd;
		volatile Throwable startException;

		volatile long stopBegin;
		volatile long stopEnd;
		volatile Throwable stopException;

		private enum Operation {
			NEW, STARTING, STARTED, STOPPING, STOPPED, EXCEPTION
		}

		Operation getOperation() {
			if (startException != null || stopException != null) return Operation.EXCEPTION;
			if (stopEnd != 0) return Operation.STOPPED;
			if (stopBegin != 0) return Operation.STOPPING;
			if (startEnd != 0) return Operation.STARTED;
			if (startBegin != 0) return Operation.STARTING;
			return Operation.NEW;
		}

		boolean isStarting() {
			return startBegin != 0 && startEnd == 0;
		}

		boolean isStarted() {
			return startEnd != 0;
		}

		boolean isStartedSuccessfully() {
			return startEnd != 0 && startException == null;
		}

		boolean isStopping() {
			return stopBegin != 0 && stopEnd == 0;
		}

		boolean isStopped() {
			return stopEnd != 0;
		}

//		boolean isStoppedSuccessfully() {
//			return stopEnd != 0 && stopException == null;
//		}

		long getStartTime() {
			checkState(startBegin != 0L && startEnd != 0L);
			return startEnd - startBegin;
		}

		long getStopTime() {
			checkState(stopBegin != 0L && stopEnd != 0L);
			return stopEnd - stopBegin;
		}
	}

	private final Map<Key<?>, NodeStatus> nodeStatuses = new ConcurrentHashMap<>();

	private String graphvizGraph = "rankdir=LR";
	private String graphvizStarting = "color=green";
	private String graphvizStarted = "color=blue";
	private String graphvizStopping = "color=green";
	private String graphvizStopped = "color=grey";
	private String graphvizException = "color=red";
	private String graphvizNodeWithSuffix = "peripheries=2";
	private String graphvizSlowestNode = "style=bold";
	private String graphvizSlowestEdge = "color=blue style=bold";
	private String graphvizEdge = "";

	private ServiceGraph() {
	}

	public static ServiceGraph create() {
		return new ServiceGraph();
	}

	public ServiceGraph withStartCallback(Runnable startCallback) {
		this.startCallback = startCallback;
		return this;
	}

	public ServiceGraph withNodeSuffixes(Function<Key<?>, ?> nodeSuffixes) {
		this.nodeSuffixes = nodeSuffixes;
		return this;
	}

	public ServiceGraph withGraphvizGraph(String graphvizGraph) {
		this.graphvizGraph = graphvizGraph;
		return this;
	}

	public ServiceGraph withGraphvizStarting(String graphvizStarting) {
		this.graphvizStarting = toGraphvizAttribute(graphvizStarting);
		return this;
	}

	public ServiceGraph withGraphvizStarted(String graphvizStarted) {
		this.graphvizStarted = toGraphvizAttribute(graphvizStarted);
		return this;
	}

	public ServiceGraph withGraphvizStopping(String graphvizStopping) {
		this.graphvizStopping = toGraphvizAttribute(graphvizStopping);
		return this;
	}

	public ServiceGraph withGraphvizStopped(String graphvizStopped) {
		this.graphvizStopped = toGraphvizAttribute(graphvizStopped);
		return this;
	}

	public ServiceGraph withGraphvizException(String graphvizException) {
		this.graphvizException = toGraphvizAttribute(graphvizException);
		return this;
	}

	public ServiceGraph withGraphvizEdge(String graphvizEdge) {
		this.graphvizEdge = toGraphvizAttribute(graphvizEdge);
		return this;
	}

	public ServiceGraph withGraphvizNodeWithSuffix(String graphvizNodeWithSuffix) {
		this.graphvizNodeWithSuffix = toGraphvizAttribute(graphvizNodeWithSuffix);
		return this;
	}

	public ServiceGraph withGraphvizSlowestNode(String graphvizSlowestNode) {
		this.graphvizSlowestNode = toGraphvizAttribute(graphvizSlowestNode);
		return this;
	}

	public ServiceGraph withGraphvizSlowestEdge(String graphvizSlowestEdge) {
		this.graphvizSlowestEdge = toGraphvizAttribute(graphvizSlowestEdge);
		return this;
	}

	private static String toGraphvizAttribute(String colorOrAttribute) {
		if (colorOrAttribute.isEmpty() || colorOrAttribute.contains("=")) return colorOrAttribute;
		return "color=" + (colorOrAttribute.startsWith("#") ? "\"" + colorOrAttribute + "\"" : colorOrAttribute);
	}

	private static Throwable getRootCause(Throwable throwable) {
		Throwable cause;
		while ((cause = throwable.getCause()) != null) {
			throwable = cause;
		}
		return throwable;
	}

	public ServiceGraph add(Key<?> key, @Nullable Service service, Key<?>... dependencies) {
		checkArgument(!services.containsKey(key));
		if (service != null) {
			services.put(key, service);
		}
		add(key, asList(dependencies));
		return this;
	}

	public ServiceGraph add(Key<?> key, Collection<Key<?>> dependencies) {
		for (Key<?> dependency : dependencies) {
			checkArgument(!(dependency instanceof Service), "Dependency %s must be a key, not a service", dependency);
			forwards.computeIfAbsent(key, o -> new HashSet<>()).add(dependency);
			backwards.computeIfAbsent(dependency, o -> new HashSet<>()).add(key);
		}
		return this;
	}

	@SuppressWarnings("unchecked")
	public ServiceGraph add(Key<?> key, Key<?> first, Key<?>... rest) {
		add(key, concat(singletonList(first), asList(rest)));
		return this;
	}

	private CompletionStage<?> processNode(Key<?> node, boolean start,
			Map<Key<?>, CompletionStage<?>> cache, Executor executor) {
		List<CompletionStage<?>> dependencies = new ArrayList<>();
		for (Key<?> dependency : (start ? forwards : backwards).getOrDefault(node, emptySet())) {
			dependencies.add(processNode(dependency, start, cache, executor));
		}

		if (cache.containsKey(node)) {
			return cache.get(node);
		}

		CompletionStage<Void> result = waitAll(dependencies)
				.thenComposeAsync($ -> {
					Service service = services.get(node);
					if (service == null) {
						logger.debug("...skipping no-service node: " + keyToString(node));
						return CompletableFuture.completedFuture(null);
					}

					if (!start && !nodeStatuses.getOrDefault(node, NodeStatus.DEFAULT).isStartedSuccessfully()) {
						logger.debug("...skipping not running node: " + keyToString(node));
						return CompletableFuture.completedFuture(null);
					}

					Stopwatch sw = Stopwatch.createStarted();
					logger.info((start ? "Starting" : "Stopping") + " node: " + keyToString(node));
					NodeStatus nodeStatus = nodeStatuses.computeIfAbsent(node, $1 -> new NodeStatus());
					if (start) {
						nodeStatus.startBegin = currentTimeMillis();
					} else {
						nodeStatus.stopBegin = currentTimeMillis();
					}
					return (start ? service.start() : service.stop())
							.whenCompleteAsync(($2, throwable) -> {
								if (start) {
									nodeStatus.startEnd = currentTimeMillis();
									nodeStatus.startException = throwable;
								} else {
									nodeStatus.stopEnd = currentTimeMillis();
									nodeStatus.stopException = throwable;
								}

								long elapsed = sw.elapsed(MILLISECONDS);
								logger.info((start ? "Started" : "Stopped") + " " + keyToString(node) + (elapsed >= 1L ? (" in " + sw) : ""));
							}, executor);
				}, executor);

		cache.put(node, result);
		return result;
	}

	private static CompletionStage<Void> waitAll(List<? extends CompletionStage<?>> stages) {
		if (stages.size() == 0) {
			return CompletableFuture.completedFuture(null);
		}
		if (stages.size() == 1) {
			return stages.get(0).thenApply($ -> null);
		}
		CompletableFuture<Void> result = new CompletableFuture<>();
		AtomicInteger atomicInteger = new AtomicInteger(stages.size());
		Set<Throwable> exceptions = new LinkedHashSet<>();
		for (CompletionStage<?> future : stages) {
			future.whenCompleteAsync(($, throwable) -> {
				if (throwable != null) {
					synchronized (exceptions) {
						exceptions.add(getRootCause(throwable));
					}
				}
				if (atomicInteger.decrementAndGet() == 0) {
					if (exceptions.isEmpty()) {
						result.complete(null);
					} else {
						Throwable e = first(exceptions);
						exceptions.stream().skip(1).forEach(e::addSuppressed);
						result.completeExceptionally(e);
					}
				}
			});
		}
		return result;
	}

	synchronized public boolean isStarted() {
		return started;
	}

	/**
	 * Start services in the service graph
	 */
	synchronized public CompletableFuture<?> startFuture() {
		if (started) return CompletableFuture.completedFuture(false);
		started = true;
		if (startCallback != null) {
			startCallback.run();
		}
		List<Key<?>> circularDependencies = findCircularDependencies();
		checkState(circularDependencies == null, "Circular dependencies found: %s", circularDependencies);
		Set<Key<?>> rootNodes = difference(union(services.keySet(), forwards.keySet()), backwards.keySet());
		logger.info("Starting services");
		logger.debug("Root nodes: {}", rootNodes);
		startBegin = currentTimeMillis();
		return doStartStop(true, rootNodes)
				.whenComplete(($, e) -> {
					startEnd = currentTimeMillis();
					if (e != null) startException = e;
				})
				.thenRun(() ->
						slowestChain = findSlowestChain(
								difference(union(services.keySet(), forwards.keySet()), backwards.keySet()),
								new HashMap<>()))
				.toCompletableFuture();
	}

	/**
	 * Stop services from the service graph
	 */
	synchronized public CompletableFuture<?> stopFuture() {
		Set<Key<?>> leafNodes = difference(union(services.keySet(), backwards.keySet()), forwards.keySet());
		logger.info("Stopping services");
		logger.debug("Leaf nodes: {}", leafNodes);
		stopBegin = currentTimeMillis();
		return doStartStop(false, leafNodes)
				.whenComplete(($, e) -> {
					stopEnd = currentTimeMillis();
					if (e != null) stopException = e;
				})
				.toCompletableFuture();
	}

	private CompletionStage<Void> doStartStop(boolean start, Collection<Key<?>> startNodes) {
		ExecutorService executor = newSingleThreadExecutor();
		Map<Key<?>, CompletionStage<?>> cache = new HashMap<>();
		return waitAll(
				startNodes.stream()
						.map(rootNode -> processNode(rootNode, start, cache, executor))
						.collect(toList()))
				.whenCompleteAsync(($, throwable) -> executor.shutdown(), executor);
	}

	private static void removeValue(Map<Key<?>, Set<Key<?>>> map, Key<?> key, Key<?> value) {
		Set<Key<?>> objects = map.get(key);
		objects.remove(value);
		if (objects.isEmpty()) map.remove(key);
	}

	private void removeIntermediateOneWay(Key<?> vertex, Map<Key<?>, Set<Key<?>>> forwards, Map<Key<?>, Set<Key<?>>> backwards) {
		for (Key<?> backward : backwards.getOrDefault(vertex, emptySet())) {
			removeValue(forwards, backward, vertex);
			for (Key<?> forward : forwards.getOrDefault(vertex, emptySet())) {
				if (!forward.equals(backward)) {
					forwards.computeIfAbsent(backward, o -> new HashSet<>()).add(forward);
				}
			}
		}
	}

	private void removeIntermediate(Key<?> vertex) {
		removeIntermediateOneWay(vertex, forwards, backwards);
		removeIntermediateOneWay(vertex, backwards, forwards);
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

	@Nullable
	private List<Key<?>> findCircularDependencies() {
		Set<Key<?>> visited = new LinkedHashSet<>();
		List<Key<?>> path = new ArrayList<>();
		next:
		while (true) {
			for (Key<?> node : path.isEmpty() ? services.keySet() : forwards.getOrDefault(path.get(path.size() - 1), emptySet())) {
				int loopIndex = path.indexOf(node);
				if (loopIndex != -1) {
					logger.warn("Circular dependencies found: " + path.subList(loopIndex, path.size()).stream()
							.map(this::keyToString)
							.collect(joining(", ", "[", "]")));
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

	private static final class SlowestChain {
		final List<Key<?>> path;
		final long sum;

		private SlowestChain(List<Key<?>> path, long sum) {
			this.path = path;
			this.sum = sum;
		}

		static SlowestChain concat(Key<?> key, long keyValue, SlowestChain prefix) {
			return new SlowestChain(CollectionUtils.concat(prefix.path, singletonList(key)), prefix.sum + keyValue);
		}

		static SlowestChain of(Key<?> key, long keyValue) {
			return new SlowestChain(singletonList(key), keyValue);
		}
	}

	private SlowestChain findSlowestChain(Collection<Key<?>> nodes, Map<Key<?>, SlowestChain> memo) {
		assert !nodes.isEmpty();
		return nodes.stream()
				.map(node -> {
					SlowestChain slowestChain = memo.get(node);
					if (slowestChain != null) {
						return slowestChain;
					}
					return forwards.containsKey(node) ?
							SlowestChain.concat(node, nodeStatuses.get(node).getStartTime(),
									findSlowestChain(forwards.get(node), memo)) :
							SlowestChain.of(node, nodeStatuses.get(node).getStartTime());
				})
				.max(comparingLong(longestPath -> longestPath.sum))
				.get();
	}

	private String keyToString(Key<?> key) {
		Annotation annotation = key.getAnnotation();
		return (annotation != null ? prettyPrintAnnotation(annotation) + " " : "") +
				key.getTypeLiteral();
	}

	private String keyToNode(Key<?> key) {
		String str = keyToString(key)
				.replace("\n", "\\n")
				.replace("\"", "\\\"");
		return "\"" + str + "\"";
	}

	private String keyToLabel(Key<?> key) {
		Annotation annotation = key.getAnnotation();
		Object nodeSuffix = nodeSuffixes.apply(key);
		NodeStatus status = nodeStatuses.get(key);
		String label = (annotation != null ? prettyPrintAnnotation(annotation) + "\\n" : "") +
				SimpleType.ofType(key.getTypeLiteral().getType()).getSimpleName() +
				(nodeSuffix != null ? " [" + nodeSuffix + "]" : "") +
				(status != null && status.isStarted() ?
						"\\n" +
								formatDuration(Duration.ofMillis(status.getStartTime())) +
								(status.isStopped() ?
										" / " + formatDuration(Duration.ofMillis(status.getStopTime())) :
										"") :
						"") +
				(status != null && status.startException != null ? "\\n" + status.startException : "") +
				(status != null && status.stopException != null ? "\\n" + status.stopException : "");
		return label.replace("\"", "\\\"");
	}

	@Override
	public String toString() {
		return toGraphViz();
	}

	@JmxOperation
	@SuppressWarnings("StringConcatenationInsideStringBufferAppend")
	public String toGraphViz() {
		StringBuilder sb = new StringBuilder();
		sb.append("digraph {\n");
		if (!graphvizGraph.isEmpty()) {
			sb.append("\t" + graphvizGraph + "\n");
		}
		for (Key<?> node : forwards.keySet()) {
			for (Key<?> dependency : forwards.get(node)) {
				sb.append("\t" + keyToNode(node) + " -> " + keyToNode(dependency) +
						(slowestChain != null &&
								slowestChain.path.contains(node) && slowestChain.path.contains(dependency) &&
								slowestChain.path.indexOf(node) == slowestChain.path.indexOf(dependency) + 1 ?
								" [" + graphvizSlowestEdge + "]" :
								(!graphvizEdge.isEmpty() ? " [" + graphvizEdge + "]" : "")) +
						"\n");
			}
		}

		Map<NodeStatus.Operation, String> nodeColors = new HashMap<>();
		nodeColors.put(NodeStatus.Operation.STARTING, graphvizStarting);
		nodeColors.put(NodeStatus.Operation.STARTED, graphvizStarted);
		nodeColors.put(NodeStatus.Operation.STOPPING, graphvizStopping);
		nodeColors.put(NodeStatus.Operation.STOPPED, graphvizStopped);
		nodeColors.put(NodeStatus.Operation.EXCEPTION, graphvizException);

		sb.append("\n");
		for (Key<?> key : union(services.keySet(), union(backwards.keySet(), forwards.keySet()))) {
			NodeStatus status = nodeStatuses.get(key);
			String nodeColor = status != null ? nodeColors.getOrDefault(status.getOperation(), "") : "";
			Object suffix = nodeSuffixes.apply(key);
			sb.append("\t" + keyToNode(key) + " [ label=\"" + keyToLabel(key) + "\"" +
					(!nodeColor.isEmpty() ? " " + nodeColor : "") +
					(suffix != null ? " " + graphvizNodeWithSuffix : "") +
					(slowestChain != null && slowestChain.path.contains(key) ? " " + graphvizSlowestNode : "") +
					" ]\n");
		}

		sb.append("\n\t{ rank=same; " +
				difference(union(services.keySet(), backwards.keySet()), forwards.keySet()).stream()
						.map(this::keyToNode)
						.collect(joining(" ")) +
				" }\n");

		sb.append("}\n");
		return sb.toString();
	}

	@JmxAttribute
	public String getStartingNodes() {
		return union(services.keySet(), union(backwards.keySet(), forwards.keySet())).stream()
				.filter(node -> {
					NodeStatus status = nodeStatuses.get(node);
					return status != null && status.isStarting();
				})
				.map(this::keyToString)
				.collect(joining(", "));
	}

	@JmxAttribute
	public String getStoppingNodes() {
		return union(services.keySet(), union(backwards.keySet(), forwards.keySet())).stream()
				.filter(node -> {
					NodeStatus status = nodeStatuses.get(node);
					return status != null && status.isStopping();
				})
				.map(this::keyToString)
				.collect(joining(", "));
	}

	@JmxAttribute
	@Nullable
	public String getSlowestNode() {
		return union(services.keySet(), union(backwards.keySet(), forwards.keySet())).stream()
				.filter(key -> {
					NodeStatus nodeStatus = nodeStatuses.get(key);
					return nodeStatus != null && nodeStatus.isStarted();
				})
				.max(comparingLong(node -> nodeStatuses.get(node).getStartTime()))
				.map(node -> keyToString(node) +
						" : " +
						formatDuration(Duration.ofMillis(nodeStatuses.get(node).getStartTime())))
				.orElse(null);
	}

	@JmxAttribute
	@Nullable
	public String getSlowestChain() {
		if (slowestChain == null) return null;
		return slowestChain.path.stream()
				.map(this::keyToString)
				.collect(joining(", ", "[", "]")) +
				" : " +
				formatDuration(Duration.ofMillis(slowestChain.sum));
	}

	@JmxAttribute
	@Nullable
	public Duration getStartDuration() {
		if (startBegin == 0) return null;
		return Duration.ofMillis((startEnd != 0 ? startEnd : currentTimeMillis()) - startBegin);
	}

	@JmxAttribute
	public Throwable getStartException() {
		return startException;
	}

	@JmxAttribute
	@Nullable
	public Duration getStopDuration() {
		if (stopBegin == 0) return null;
		return Duration.ofMillis((stopEnd != 0 ? stopEnd : currentTimeMillis()) - stopBegin);
	}

	@JmxAttribute
	public Throwable getStopException() {
		return stopException;
	}

}
