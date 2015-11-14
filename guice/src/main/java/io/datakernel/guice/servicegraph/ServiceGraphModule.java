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

package io.datakernel.guice.servicegraph;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.SetMultimap;
import com.google.inject.*;
import com.google.inject.internal.MoreTypes;
import com.google.inject.spi.Dependency;
import com.google.inject.spi.HasDependencies;
import io.datakernel.guice.workers.NioWorkerScope;
import io.datakernel.guice.workers.NioWorkerScopeFactory;
import io.datakernel.guice.workers.WorkerThread;
import io.datakernel.service.AsyncService;
import io.datakernel.service.AsyncServiceCallback;
import io.datakernel.service.ServiceGraph;
import org.slf4j.Logger;

import java.lang.annotation.Annotation;
import java.util.*;
import java.util.concurrent.Executor;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.collect.Sets.*;
import static org.slf4j.LoggerFactory.getLogger;

public final class ServiceGraphModule extends AbstractModule {
	private static final Logger logger = getLogger(ServiceGraphModule.class);

	private final Map<Class<?>, ServiceGraphFactory<?>> factoryMap = new LinkedHashMap<>();

	private final Map<Key<?>, ServiceGraphFactory<?>> keys = new LinkedHashMap<>();

	private final SetMultimap<Key<?>, Key<?>> addedDependencies = HashMultimap.create();
	private final SetMultimap<Key<?>, Key<?>> removedDependencies = HashMultimap.create();

	private final Executor executor;

	/**
	 * Creates a new instance of ServiceGraphModule with default executor
	 */
	public ServiceGraphModule() {
		this.executor = new ThreadPoolExecutor(0, Integer.MAX_VALUE,
				10, TimeUnit.MILLISECONDS,
				new SynchronousQueue<Runnable>());
	}

	/**
	 * Creates instance of ServiceGraphModule with executor
	 *
	 * @param executor executor which will be execute tasks
	 */
	public ServiceGraphModule(Executor executor) {
		this.executor = executor;
	}

	/**
	 * Puts an instance of class and its factory to the factoryMap
	 *
	 * @param <T>     type of service
	 * @param type    key with which the specified factory is to be associated
	 * @param factory value to be associated with the specified type
	 * @return ServiceGraphModule with change
	 */
	public <T> ServiceGraphModule serviceForAssignableClasses(Class<T> type, ServiceGraphFactory<T> factory) {
		factoryMap.put(type, factory);
		return this;
	}

	/**
	 * Puts the key and its factory to the keys
	 *
	 * @param key     key with which the specified factory is to be associated
	 * @param factory value to be associated with the specified key
	 * @param <T>     type of service
	 * @return ServiceGraphModule with change
	 */

	public <T> ServiceGraphModule serviceForSpecificKey(Key<T> key, ServiceGraphFactory<T> factory) {
		keys.put(key, factory);
		return this;
	}

	/**
	 * Adds the dependency for key
	 *
	 * @param key           key for adding dependency
	 * @param keyDependency key of dependency
	 * @return ServiceGraphModule with change
	 */
	public ServiceGraphModule addDependency(Key<?> key, Key<?> keyDependency) {
		addedDependencies.put(key, keyDependency);
		return this;
	}

	/**
	 * Removes the dependency
	 *
	 * @param key           key for removing dependency
	 * @param keyDependency key of dependency
	 * @return ServiceGraphModule with change
	 */
	public ServiceGraphModule removeDependency(Key<?> key, Key<?> keyDependency) {
		removedDependencies.put(key, keyDependency);
		return this;
	}

	public static class KeyInPool {
		private final Key<?> key;
		private final int index;

		public KeyInPool(Key<?> key, int index) {
			this.key = key;
			this.index = index;
		}

		@Override
		public boolean equals(Object o) {
			if (this == o) return true;
			if (o == null || getClass() != o.getClass()) return false;

			KeyInPool keyInPool = (KeyInPool) o;

			if (index != keyInPool.index) return false;
			return !(key != null ? !key.equals(keyInPool.key) : keyInPool.key != null);

		}

		@Override
		public int hashCode() {
			int result = key != null ? key.hashCode() : 0;
			result = 31 * result + index;
			return result;
		}

		@Override
		public String toString() {
			return key.getTypeLiteral().toString();
		}
	}

	@SuppressWarnings("unchecked")
	private AsyncService getServiceFromNioScopeInstanceOrNull(Key<?> key, Object instance) {
		checkNotNull(instance);
		ServiceGraphFactory<?> factoryForKey = keys.get(key);
		if (factoryForKey != null) {
			return ((ServiceGraphFactory<Object>) factoryForKey).getService(instance, executor);
		}
		for (Class<?> type : factoryMap.keySet()) {
			if (type.isAssignableFrom(instance.getClass())) {
				ServiceGraphFactory<?> serviceGraphFactory = factoryMap.get(type);
				AsyncService service = ((ServiceGraphFactory<Object>) serviceGraphFactory).getService(instance, executor);
				return checkNotNull(service);
			}
		}
		return null;
	}

	private ServiceGraph.Node nodeFromNioScope(Key<?> key, Object instance, int poolIndex) {
		return new ServiceGraph.Node(new KeyInPool(key, poolIndex), getServiceFromNioScopeInstanceOrNull(key, instance));
	}

	@SuppressWarnings("unchecked")
	private AsyncService getServiceOrNull(Key<?> key, Injector injector) {
		Binding<?> binding = injector.getBinding(key);
		if (!Scopes.isSingleton(binding)) return null;
		Object object = injector.getInstance(key);

		ServiceGraphFactory<?> factoryForKey = keys.get(key);
		if (factoryForKey != null) {
			checkNotNull(object, "SingletonService object is not instantiated for " + key);
			AsyncService service = ((ServiceGraphFactory<Object>) factoryForKey).getService(object, executor);
			return checkNotNull(service);
		}
		if (object == null)
			return null;
		for (Class<?> type : factoryMap.keySet()) {
			if (type.isAssignableFrom(object.getClass())) {
				ServiceGraphFactory<?> serviceGraphFactory = factoryMap.get(type);
				AsyncService service = ((ServiceGraphFactory<Object>) serviceGraphFactory).getService(object, executor);
				return checkNotNull(service);
			}
		}
		if (binding instanceof HasDependencies) {
			Set<Dependency<?>> dependencies = ((HasDependencies) binding).getDependencies();
			for (Dependency<?> dependency : dependencies) {
				// TODO (vsavchuk) improve
				if (dependency.getKey().equals(Key.get(NioWorkerScopeFactory.class))) {
					return containerOfServices();
				}
			}
		}
		return null;
	}

	private ServiceGraph.Node nodeFromService(Key<?> key, Injector injector) {
		return new ServiceGraph.Node(key, getServiceOrNull(key, injector));
	}

	private Key<?> sameKey(Injector injector, List list) {
		Key<?> listKey = null;
		for (Object object : list) {
			Key<?> keyFromObject = Key.get(object.getClass());
			if (listKey == null) {
				listKey = keyFromObject;
			} else {
				if (!listKey.equals(keyFromObject)) return null;
			}
		}
		return listKey;
	}

	private void createGuiceGraph(final Injector injector, final NioWorkerScope nioWorkerScope, final ServiceGraph graph) {
		if (!difference(keys.keySet(), injector.getAllBindings().keySet()).isEmpty()) {
			logger.warn("Unused keys : {}", keys.keySet());
		}

		final List<Map<Key<?>, Object>> pool = nioWorkerScope.getPool();
		final Map<Class<?>, List<ServiceGraphModule.KeyInPool>> poolObjectKey = nioWorkerScope.getPoolMapClassKey();

		for (final Key<?> key : injector.getAllBindings().keySet()) {
			final ServiceGraph.Node serviceNode = nodeFromService(key, injector);

			graph.add(serviceNode);
			Set<Key<?>> dependenciesForKey = new HashSet<>();
			processDependencies(serviceNode, key, injector, graph, dependenciesForKey, new AddDependence() {
				@Override
				public void add(Key<?> dependencyKey) {
					// TODO (vsavchuk) improve
					if (dependencyKey.equals(Key.get(NioWorkerScopeFactory.class))) {
						// TODO (vsavchuk) improve
						Set<Dependency<?>> dependencies = ((HasDependencies) injector.getBinding(key)).getDependencies();
						for (Dependency<?> dependency : dependencies) {
							Key<?> depKey = dependency.getKey();
							if (depKey.getTypeLiteral().getRawType().equals(Provider.class)
									&& depKey.getAnnotationType().equals(WorkerThread.class)) {
								Class<?> actualTypeArguments = (Class<?>)((MoreTypes.ParameterizedTypeImpl)
										depKey.getTypeLiteral().getType()).getActualTypeArguments()[0];
								for (KeyInPool actualKeyInPool : poolObjectKey.get(actualTypeArguments)) {
									ServiceGraph.Node dependencyNode = nodeFromNioScope(actualKeyInPool.key,
											pool.get(actualKeyInPool.index).get(actualKeyInPool.key), actualKeyInPool.index);
									graph.add(serviceNode, dependencyNode);
								}
							}
						}
					}
				}
			});
		}

		for (int poolIndex = 0; poolIndex < pool.size(); poolIndex++) {
			Map<Key<?>, Object> nioScopeMap = pool.get(poolIndex);
			createNioScopeGraph(nioScopeMap, injector, graph, poolIndex);
		}
	}

	private interface AddDependence {
		void add(Key<?> dependencyKey);
	}

	private void processDependencies(ServiceGraph.Node serviceNode, Key<?> key, Injector injector, ServiceGraph graph,
	                                 Set<Key<?>> dependenciesForKey, AddDependence addDependence) {
		Binding<?> binding = injector.getBinding(key);
		if (binding instanceof HasDependencies) {
			Set<Dependency<?>> dependencies = ((HasDependencies) binding).getDependencies();
			Set<Key<?>> removedDependenciesForKey = newHashSet(removedDependencies.get(key));
			for (Dependency<?> dependency : dependencies) {
				Key<?> dependencyKey = dependency.getKey();
				dependenciesForKey.add(dependencyKey);
				if (removedDependenciesForKey.contains(dependencyKey)) {
					removedDependenciesForKey.remove(dependencyKey);
					continue;
				}

				addDependence.add(dependencyKey);

				ServiceGraph.Node dependencyNode = nodeFromService(dependencyKey, injector);
				graph.add(serviceNode, dependencyNode);

				if (!removedDependenciesForKey.isEmpty()) {
					logger.warn("Unused removed dependencies for {} : {}", key, removedDependenciesForKey);
				}
			}
			if (!intersection(dependenciesForKey, addedDependencies.get(key)).isEmpty()) {
				logger.warn("Duplicate added dependencies for {} : {}", key, intersection(dependenciesForKey, addedDependencies.get(key)));
			}

			for (Key<?> dependencyKey : difference(addedDependencies.get(key), dependenciesForKey)) {
				ServiceGraph.Node dependencyNode = nodeFromService(dependencyKey, injector);
				graph.add(serviceNode, dependencyNode);
			}
		}
	}

	private void createNioScopeGraph(final Map<Key<?>, Object> map, Injector injector, final ServiceGraph graph, final int poolIndex) {
		for (Key<?> key : map.keySet()) {
			Object nioScopeObject = map.get(key);
			final ServiceGraph.Node serviceNode = nodeFromNioScope(key, nioScopeObject, poolIndex);

			graph.add(serviceNode);
			Set<Key<?>> dependenciesForKey = new HashSet<>();
			processDependencies(serviceNode, key, injector, graph, dependenciesForKey, new AddDependence() {

				@Override
				public void add(Key<?> dependencyKey) {
					if (map.containsKey(dependencyKey)) {
						Object nioScopeDependencyObject = map.get(dependencyKey);
						ServiceGraph.Node node = nodeFromNioScope(dependencyKey, nioScopeDependencyObject, poolIndex);
						graph.add(serviceNode, node);
					}
				}
			});
		}
	}

	@Override
	protected void configure() {
	}

	public static ServiceGraph getServiceGraph(Injector injector, Key<?>... rootKeys) {
		for (Key<?> rootKey : rootKeys) {
			injector.getInstance(rootKey);
		}
		return injector.getBinding(ServiceGraph.class).getProvider().get();
	}

	public static ServiceGraph getServiceGraph(Injector injector, Class<?>... rootClasses) {
		for (Class<?> rootClass : rootClasses) {
			injector.getInstance(rootClass);
		}
		return injector.getBinding(ServiceGraph.class).getProvider().get();
	}

	/**
	 * Creates the new ServiceGraph without  circular dependencies and intermediate nodes
	 *
	 * @param injector injector for building the graphs of objects
	 * @return created ServiceGraph
	 */
	@Provides
	@Singleton
	ServiceGraph serviceGraph(final Injector injector, final NioWorkerScope nioWorkerScope) {
		return new ServiceGraph() {
			@Override
			protected void onStart() {
				createGuiceGraph(injector, nioWorkerScope, this);
				logger.debug("Dependencies graph: \n" + this);
				removeIntermediateNodes();
				breakCircularDependencies();
				logger.info("Services graph: \n" + this);
			}

			@Override
			protected String nodeToString(Node node) {
				Object key = node.getKey();
				if (key instanceof Key) {
					Key<?> guiceKey = (Key<?>) key;
					Annotation annotation = guiceKey.getAnnotation();
					return guiceKey.getTypeLiteral() +
							(annotation != null ? " " + annotation : "");
				}
				return super.nodeToString(node);
			}
		};
	}

	private AsyncService containerOfServices() {
		return new AsyncService() {
			@Override
			public void start(AsyncServiceCallback callback) {
				callback.onComplete();
			}

			@Override
			public void stop(AsyncServiceCallback callback) {
				callback.onComplete();
			}
		};
	}
}