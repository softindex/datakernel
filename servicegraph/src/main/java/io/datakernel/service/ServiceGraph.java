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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import util.Preconditions;
import util.Stopwatch;

import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;

import static java.lang.System.currentTimeMillis;
import static java.util.Collections.shuffle;
import static java.util.concurrent.Executors.newSingleThreadExecutor;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

public class ServiceGraph {

	/**
	 * Inner class which represents service with its identification key - object. This object can
	 * be other service.
	 */
	public static final class Node {
		private final Object key;
		private final AsyncService service;

		public Node(Object key, AsyncService service) {
			this.key = key;
			this.service = service;
		}

		public Node(Object key) {
			this.key = key;
			this.service = null;
		}

		@Override
		public boolean equals(Object o) {
			if (this == o) return true;
			if (o == null || getClass() != o.getClass()) return false;

			Node that = (Node) o;

			return key.equals(that.key);
		}

		@Override
		public int hashCode() {
			return key.hashCode();
		}

		@Override
		public String toString() {
			return key.toString();
		}

		public Object getKey() {
			return key;
		}

		public AsyncService getService() {
			return service;
		}
	}

	private static final Logger logger = LoggerFactory.getLogger(ServiceGraph.class);

	private final ThreadFactory threadFactory;

	/**
	 * It represents the set of Node - services with keys and in this time it is set of vertices for
	 * this service graph
	 */
	private final Set<Node> vertices = new LinkedHashSet<>();

	/**
	 * This set used to represent edges between vertices. If N1 and N2 - nodes and between them
	 * exists edge from N1 to N2, it can be represent as adding to this SetMultimap element <N1,N2>.
	 * This collection consist of nodes in which there are edges and their keys - previous nodes.
	 */

	private final Map<Node, Set<Node>> forwards = new LinkedForwardMap();

	/**
	 * This set used to represent edges between vertices. If N1 and N2 - nodes and between them
	 * exists edge from N1 to N2, it can be represent as adding to this SetMultimap element <N2,N1>
	 * This collection consist of nodes in which there are edges and their keys - previous nodes.
	 */

	private final Map<Node, Set<Node>> backwards = new LinkedForwardMap();

	/**
	 * Services which have been started
	 */
	private final LinkedHashSet<Node> startedServices = new LinkedHashSet<>();

	/**
	 * Services which have been failed before starting will be added there
	 */
	private final LinkedHashMap<Node, Throwable> failedServices = new LinkedHashMap<>();

	private boolean started;

	public ServiceGraph() {
		this.threadFactory = Executors.defaultThreadFactory();
	}

	public ServiceGraph(ThreadFactory threadFactory) {
		this.threadFactory = threadFactory;
	}

	/**
	 * Adds the service with dependencies to service graph. It can do it only if this service have not
	 * been started yet.
	 *
	 * @param service      node which will be added
	 * @param dependencies service depends on this list of node
	 * @return changed service graph
	 */
	public final ServiceGraph add(Node service, Node... dependencies) {
		Preconditions.check(!started, "Already started");
		return add(service, Arrays.asList(dependencies));
	}

	/**
	 * Adds the service with dependencies from the specified iterable to service graph. It can do
	 * it only if this service have not been started yet.
	 *
	 * @param service      node which will be added
	 * @param dependencies service depends on this iterable list of node
	 * @return changed service graph
	 */
	public ServiceGraph add(Node service, Iterable<Node> dependencies) {
		Preconditions.check(!started, "Already started");
		vertices.add(service);
		for (Node dependency : dependencies) {
			vertices.add(dependency);
			forwards.get(service).add(dependency);
			backwards.get(dependency).add(service);
		}
		return this;
	}

	private static List<Node> nextNodes(Set<Node> processedNodes,
	                                    Set<Node> vertices, Map<Node, Set<Node>> directNodes, Map<Node, Set<Node>> backwardNodes) {
		List<Node> result = new ArrayList<>();
		for (Node node : vertices) {
			if (processedNodes.contains(node))
				continue;
			boolean found = true;
			for (Node backwardNode : backwardNodes.get(node)) {
				if (!processedNodes.contains(backwardNode)) {
					found = false;
					break;
				}
			}
			if (found) {
				result.add(node);
			}
		}
		return result;
	}

	/**
	 * Interface which represent an action with each node from graph
	 */
	public interface ServiceGraphAction {
		/**
		 * Executes the action for service from argument. Used under traversing the graph.
		 */
		void asyncAction(Node service, AsyncServiceCallback serviceCallback);
	}

	private void longestPath(Map<Node, Long> timings,
	                         Set<Node> vertices, Map<Node, Set<Node>> forwardNodes, Map<Node, Set<Node>> backwardNodes) {
		List<Node> stack = new ArrayList<>();
		List<Iterator<Node>> path = new ArrayList<>();
		path.add(difference(vertices, (backwardNodes.keySet())).iterator());
		int length = 0;
		int maxLength = -1;
		List<Node> maxLengthStack = null;
		while (!path.isEmpty()) {
			assert path.size() != 1 || (length == 0 && stack.isEmpty());

			Iterator<Node> it = path.get(path.size() - 1);
			if (it.hasNext()) {
				Node node = it.next();
				if (stack.contains(node)) {
					stack.add(node);
					logger.warn("Could not calculate longest path, only DAGs are supported now. Looped on: {}", stack);
					return;
				}
				Long timing = timings.get(node);
				length += 1 + (timing != null ? timing : 0);
				stack.add(node);
				path.add(forwardNodes.get(node).iterator());
			} else {
				if (length > maxLength) {
					maxLength = length;
					maxLengthStack = new ArrayList<>(stack);
				}
				if (!stack.isEmpty()) {
					Long timing = timings.get(stack.get(stack.size() - 1));
					length -= 1 + (timing != null ? timing : 0);
					stack.remove(stack.size() - 1);
				}
				path.remove(path.size() - 1);
			}
		}

		assert length == 0 && stack.isEmpty();

		if (maxLengthStack != null) {
			StringBuilder sb = new StringBuilder();
			for (Node node : maxLengthStack) {
				Long timing = timings.get(node);
				sb.append(nodeToString(node)).append(" : ");
				sb.append(String.format("%1.3f sec", (timing != null ? timing / 1000.0 : 0)));
				sb.append("\n");
			}
			logger.info("Longest path:\n" + sb);
		}
	}

	synchronized private void next(final ServiceGraphAction action, final ExecutorService executorService,
	                               final Set<Node> activeNodes, final Set<Node> processedNodes, final Map<Node, Throwable> failedNodes,
	                               final Set<Node> vertices, final Map<Node, Set<Node>> forwardNodes, final Map<Node, Set<Node>> backwardNodes,
	                               final Map<Node, Long> processingTimes, final AsyncServiceCallback callback, final String done, final String fail) {
		List<Node> newNodes = Collections.emptyList();
		if (failedNodes.isEmpty()) {
			newNodes = nextNodes(processedNodes, vertices, forwardNodes, backwardNodes);
			newNodes.removeAll(activeNodes);
		}

		if (newNodes.isEmpty()) {
			if (activeNodes.isEmpty()) {
				executorService.shutdown();
				if (failedNodes.isEmpty()) {
					callback.onComplete();
					longestPath(processingTimes, vertices, forwardNodes, backwardNodes);
				} else {
					callback.onException((Exception) (failedNodes.values().iterator().next()));
				}
			}
			return;
		}

		shuffle(newNodes);

		logger.info("Processing " + nodesToString(newNodes));

		activeNodes.addAll(newNodes);

		for (final Node node : newNodes) {
			final long startProcessingTime = currentTimeMillis();

			Stopwatch asyncActionTime = Stopwatch.createStarted();
			final Stopwatch sw = Stopwatch.createStarted();

			AsyncServiceCallback callbackAction = new AsyncServiceCallback() {
				@Override
				public void onComplete() {
					synchronized (ServiceGraph.this) {
						logger.info(done + " " + nodeToString(node) + (sw.elapsed(MILLISECONDS) >= 1L ? (" in " + sw) : ""));
						processingTimes.put(node, currentTimeMillis() - startProcessingTime);
						activeNodes.remove(node);
						processedNodes.add(node);
						next(action, executorService, activeNodes, processedNodes, failedNodes, vertices, forwardNodes, backwardNodes, processingTimes, callback, done, fail);
					}
				}

				@Override
				public void onException(Exception e) {
					synchronized (ServiceGraph.this) {
						logger.error(fail + " " + nodeToString(node) + (sw.elapsed(MILLISECONDS) >= 1L ? (" in " + sw) : ""));
						processingTimes.put(node, currentTimeMillis() - startProcessingTime);
						activeNodes.remove(node);
						failedNodes.put(node, e);
						next(action, executorService, activeNodes, processedNodes, failedNodes, vertices, forwardNodes, backwardNodes, processingTimes, callback, done, fail);
					}
				}
			};
			action.asyncAction(node, callbackAction);
			if (asyncActionTime.elapsed(TimeUnit.SECONDS) >= 1)
				logger.info("action.asyncAction time for {} is {}", node, asyncActionTime);
		}
	}

	/**
	 * Called before starting execution service graph
	 */
	protected void onStart() {
	}

	public void start() throws Exception {
		AsyncServiceCallbacks.BlockingServiceCallback startCallback = AsyncServiceCallbacks.withCountDownLatch();
		start(startCallback);
		startCallback.await();
	}

	public void stop() throws Exception {
		AsyncServiceCallbacks.BlockingServiceCallback stopCallback = AsyncServiceCallbacks.withCountDownLatch();
		stop(stopCallback);
		stopCallback.await();
	}

	/**
	 * Started services from the service graph
	 */
	synchronized public void start(AsyncServiceCallback callback) {
		if (!started) {
			onStart();
			started = true;
		}
		logger.info("Starting services...");
		visitBackwardAsync(new ServiceGraphAction() {
			@Override
			public void asyncAction(final Node service, final AsyncServiceCallback callback) {
				AsyncService serviceOrNull = service.getService();
				if (serviceOrNull == null) {
					callback.onComplete();
					return;
				}

				serviceOrNull.start(callback);
			}
		}, vertices, startedServices, failedServices, callback, "started", "failed");
	}

	/**
	 * Stops services from  the service graph
	 */
	synchronized public void stop(final AsyncServiceCallback callback) {
		logger.info("Stopping running services: " + nodesToString(startedServices));
		visitForwardAsync(new ServiceGraphAction() {
			@Override
			public void asyncAction(final Node service, final AsyncServiceCallback callback) {
				AsyncService serviceOrNull = service.getService();
				if (serviceOrNull == null) {
					callback.onComplete();
					return;
				}

				serviceOrNull.stop(callback);
			}
		}, startedServices, difference(vertices, startedServices), new LinkedHashMap<Node, Throwable>(), callback, "stopped", "failed");
	}

	private Set<Node> difference(Set<Node> main, Set<Node> other) {
		Set<Node> set = new HashSet<>();
		for (Node mainNode : main) {
			if (!other.contains(mainNode)) {
				set.add(mainNode);
			}
		}
		return set;
	}

	/**
	 * Visits nodes in which there are edges from processedNodes and executes its service .
	 *
	 * @param action         action which will be executed with node
	 * @param vertices       set of all vertices
	 * @param processedNodes nodes which have been visited
	 * @param failedNodes    nodes which have not started
	 * @return SettableFuture  with result of action
	 */
	public void visitForwardAsync(final ServiceGraphAction action, final Set<Node> vertices, final Set<Node> processedNodes,
	                              final Map<Node, Throwable> failedNodes, final AsyncServiceCallback callback,
	                              final String done, final String fail) {
		final ExecutorService executor = newSingleThreadExecutor(threadFactory);
		executor.execute(new Runnable() {
			@Override
			public void run() {
				HashMap<Node, Long> processingTimes = new HashMap<>();
				next(action, executor, new HashSet<Node>(), processedNodes, failedNodes, vertices, forwards, backwards, processingTimes, callback, done, fail);
			}
		});
	}

	/**
	 * Visits nodes from which there are edges to processedNodes and executes its service .
	 *
	 * @param action         action which will be executed with node
	 * @param vertices       set of all vertices
	 * @param processedNodes nodes which have been visited
	 * @param failedNodes    nodes which have not started
	 * @return SettableFuture  with result of action
	 */
	public void visitBackwardAsync(final ServiceGraphAction action, final Set<Node> vertices, final Set<Node> processedNodes,
	                               final Map<Node, Throwable> failedNodes, final AsyncServiceCallback callback,
	                               final String done, final String fail) {
		final ExecutorService executor = newSingleThreadExecutor(threadFactory);
		executor.execute(new Runnable() {
			@Override
			public void run() {
				HashMap<Node, Long> processingTimes = new HashMap<>();
				next(action, executor, new HashSet<Node>(), processedNodes, failedNodes, vertices, backwards, forwards, processingTimes, callback, done, fail);
			}
		});

	}

	@Override
	@SuppressWarnings("StringConcatenationInsideStringBufferAppend")
	public String toString() {
		StringBuilder sb = new StringBuilder();
		Set<Node> visited = new LinkedHashSet<>();
		List<Iterator<Node>> path = new ArrayList<>();
		Iterable<Node> roots = difference(vertices, (backwards.keySet()));
		path.add(roots.iterator());
		while (!path.isEmpty()) {
			Iterator<Node> it = path.get(path.size() - 1);
			if (it.hasNext()) {
				Node node = it.next();
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

	private static String repeat(String s, int count) {
		Preconditions.checkNotNull(s);
		StringBuilder builder = new StringBuilder();

		while (count-- > 0) {
			builder.append(s);
		}

		return builder.toString();
	}

	private void removeIntermediate(Node vertex) {
		for (Node backward : backwards.get(vertex)) {
			forwards.get(backward).remove(vertex);
			for (Node forward : forwards.get(vertex)) {
				if (!forward.equals(backward)) {
					forwards.get(backward).add(forward);
				}
			}
		}
		for (Node forward : forwards.get(vertex)) {
			backwards.get(forward).remove(vertex);
			for (Node backward : backwards.get(vertex)) {
				if (!forward.equals(backward)) {
					backwards.get(forward).add(backward);
				}
			}
		}

		forwards.remove(vertex);
		backwards.remove(vertex);
		vertices.remove(vertex);
	}

	/**
	 * Removes nodes which don't have services
	 */
	public void removeIntermediateNodes() {
		Preconditions.check(!started, "Already started");
		List<Node> toRemove = new ArrayList<>();
		for (Node v : vertices) {
			if (v.getService() == null) {
				toRemove.add(v);
			}
		}

		for (Node v : toRemove) {
			removeIntermediate(v);
		}
	}

	/**
	 * Handles the situation when few nodes forming a dependency circle. Breaks circular
	 * dependencies.
	 */
	public void breakCircularDependencies() {
		Preconditions.check(!started, "Already started");
		Set<Node> visited = new LinkedHashSet<>();
		List<Node> path = new ArrayList<>();
		next:
		while (true) {
			for (Node node : path.isEmpty() ? vertices : forwards.get(path.get(path.size() - 1))) {
				int loopIndex = path.indexOf(node);
				if (loopIndex != -1) {
					logger.warn("Found circular dependency, breaking: " + nodesToString(path.subList(loopIndex, path.size())));
					Node last = path.get(path.size() - 1);
					forwards.get(last).remove(node);
					backwards.get(node).remove(last);
					continue next;
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
	}

	protected String nodeToString(Node node) {
		return node.toString();
	}

	private String nodesToString(Iterable<Node> newNodes) {
		StringBuilder builder = new StringBuilder().append("[");
		Iterator<Node> iterator = newNodes.iterator();

		while (iterator.hasNext()) {
			builder.append(nodeToString(iterator.next()));
			if (iterator.hasNext()) {
				builder.append(", ");
			}
		}
		return builder.append("]").toString();
	}

	private static class LinkedForwardSet implements Set<Node> {
		private final Set<Node> set = new LinkedHashSet<>();
		private final LinkedForwardMap referenceToMap;
		private final Node keyInMap;

		public LinkedForwardSet(LinkedForwardMap referenceToMap, Node keyInMap) {
			this.referenceToMap = referenceToMap;
			this.keyInMap = keyInMap;
		}

		@Override
		public int size() {
			return set.size();
		}

		@Override
		public boolean isEmpty() {
			return set.isEmpty();
		}

		@Override
		public boolean contains(Object o) {
			return set.contains(o);
		}

		@Override
		public Iterator<Node> iterator() {
			return set.iterator();
		}

		@Override
		public Object[] toArray() {
			return set.toArray();
		}

		@Override
		public <T> T[] toArray(T[] ts) {
			return set.toArray(ts);
		}

		@Override
		public boolean add(Node node) {
			if (!referenceToMap.containsKey(keyInMap)) {
				referenceToMap.putForwardSet(keyInMap, this);
			}
			return set.add(node);
		}

		@Override
		public boolean remove(Object o) {
			boolean result = set.remove(o);
			if (set.isEmpty() && referenceToMap.containsKey(keyInMap)) {
				referenceToMap.remove(keyInMap);
			}
			return result;
		}

		@Override
		public boolean containsAll(Collection<?> collection) {
			return set.containsAll(collection);
		}

		@Override
		public boolean addAll(Collection<? extends Node> collection) {
			return set.addAll(collection);
		}

		@Override
		public boolean retainAll(Collection<?> collection) {
			return set.retainAll(collection);
		}

		@Override
		public boolean removeAll(Collection<?> collection) {
			return set.removeAll(collection);
		}

		@Override
		public void clear() {
			set.clear();
			if (referenceToMap.containsKey(keyInMap)) referenceToMap.remove(keyInMap);
		}

		@Override
		public String toString() {
			return set.toString();
		}

		@Override
		public int hashCode() {
			return set.hashCode();
		}

		@Override
		public boolean equals(Object o) {
			if (this == o) return true;
			if (o == null || getClass() != o.getClass()) return false;

			LinkedForwardSet nodes = (LinkedForwardSet) o;

			return !(set != null ? !set.equals(nodes.set) : nodes.set != null);

		}
	}

	private static class LinkedForwardMap implements Map<Node, Set<Node>> {
		private final Map<Node, Set<Node>> map = new LinkedHashMap<>();

		@Override
		public int size() {
			return map.size();
		}

		@Override
		public boolean isEmpty() {
			return map.isEmpty();
		}

		@Override
		public boolean containsKey(Object o) {
			return map.containsKey(o);
		}

		@Override
		public boolean containsValue(Object o) {
			return map.containsValue(o);
		}

		@Override
		public Set<Node> get(Object o) {
			if (!map.containsKey(o)) {
				return new LinkedForwardSet(this, (Node) o);
			} else {
				return map.get(o);
			}
		}

		@Override
		public Set<Node> put(Node node, Set<Node> nodes) {
			if (nodes.isEmpty()) return new LinkedForwardSet(this, node);
			Set<Node> forwardSet = new LinkedForwardSet(this, node);
			forwardSet.addAll(nodes);
			return map.put(node, forwardSet);
		}

		public void putForwardSet(Node node, LinkedForwardSet nodes) {
			map.put(node, nodes);
		}

		@Override
		public Set<Node> remove(Object o) {
			return map.remove(o);
		}

		@Override
		public void putAll(Map<? extends Node, ? extends Set<Node>> map) {
			this.map.putAll(map);
		}

		@Override
		public void clear() {
			map.clear();
		}

		@Override
		public Set<Node> keySet() {
			return map.keySet();
		}

		@Override
		public Collection<Set<Node>> values() {
			return map.values();
		}

		@Override
		public Set<Entry<Node, Set<Node>>> entrySet() {
			return map.entrySet();
		}

		@Override
		public String toString() {
			return map.toString();
		}

		@Override
		public int hashCode() {
			return map.hashCode();
		}

		@Override
		public boolean equals(Object o) {
			if (this == o) return true;
			if (o == null || getClass() != o.getClass()) return false;

			LinkedForwardMap that = (LinkedForwardMap) o;

			return !(map != null ? !map.equals(that.map) : that.map != null);

		}
	}
}
