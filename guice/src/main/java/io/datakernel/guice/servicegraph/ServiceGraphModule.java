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
import com.google.inject.spi.Dependency;
import com.google.inject.spi.HasDependencies;
import io.datakernel.service.ConcurrentService;
import io.datakernel.service.ServiceGraph;
import org.slf4j.Logger;

import java.lang.annotation.Annotation;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;
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

	private final Map<Key<?>, Object> services = new LinkedHashMap<>();

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
	public <T> ServiceGraphModule factory(Class<T> type, ServiceGraphFactory<T> factory) {
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
	public <T> ServiceGraphModule factoryForKey(Key<T> key, ServiceGraphFactory<T> factory) {
		keys.put(key, factory);
		return this;
	}

	/**
	 * Puts the key and factory for service from argument to the keys
	 *
	 * @param key     key with which the specified service is to be associated
	 * @param service value to be associated with the specified key
	 * @param <T>     type of service
	 * @return ServiceGraphModule with change
	 */
	public <T> ServiceGraphModule serviceForKey(Key<T> key, final ConcurrentService service) {
		return factoryForKey(key, new ServiceGraphFactory<T>() {
			@Override
			public ConcurrentService getService(T node, Executor executor) {
				return service;
			}
		});
	}

	/**
	 * Puts the key and the null-factory to keys
	 *
	 * @param <T> type of service
	 * @param key key with which null is to be associated
	 * @return ServiceGraphModule with change
	 */
	public <T> ServiceGraphModule noServiceForKey(Key<T> key) {
		return serviceForKey(key, null);
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

	@SuppressWarnings("unchecked")
	private ConcurrentService getServiceOrNull(Key<?> key) {
		Object object = services.get(key);
		ServiceGraphFactory<?> factoryForKey = keys.get(key);
		if (factoryForKey != null) {
			checkNotNull(object, "SingletonService object is not instantiated for " + key);
			ConcurrentService service = ((ServiceGraphFactory<Object>) factoryForKey).getService(object, executor);
			return checkNotNull(service);
		}
		if (object == null)
			return null;
		for (Class<?> type : factoryMap.keySet()) {
			if (type.isAssignableFrom(object.getClass())) {
				ServiceGraphFactory<?> serviceGraphFactory = factoryMap.get(type);
				ConcurrentService service = ((ServiceGraphFactory<Object>) serviceGraphFactory).getService(object, executor);
				return checkNotNull(service);
			}
		}
		throw new IllegalArgumentException("Could not find factory for service " + key);
	}

	private void createGuiceGraph(Injector injector, ServiceGraph graph) {
		if (!difference(keys.keySet(), injector.getAllBindings().keySet()).isEmpty()) {
			logger.warn("Unused keys : {}", keys.keySet());
		}

		for (Key<?> key : injector.getAllBindings().keySet()) {
			Binding<?> binding = injector.getBinding(key);

			ServiceGraph.Node serviceNode = new ServiceGraph.Node(key,
					getServiceOrNull(key));
			graph.add(serviceNode);

			Set<Key<?>> dependenciesForKey = new HashSet<>();

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
					ServiceGraph.Node dependencyNode = new ServiceGraph.Node(dependencyKey,
							getServiceOrNull(dependencyKey));
					graph.add(serviceNode, dependencyNode);
				}
				if (!removedDependenciesForKey.isEmpty()) {
					logger.warn("Unused removed dependencies for {} : {}", key, removedDependenciesForKey);
				}
			}

			if (!intersection(dependenciesForKey, addedDependencies.get(key)).isEmpty()) {
				logger.warn("Duplicate added dependencies for {} : {}", key, intersection(dependenciesForKey, addedDependencies.get(key)));
			}

			for (Key<?> dependencyKey : difference(addedDependencies.get(key), dependenciesForKey)) {
				ServiceGraph.Node dependencyNode = new ServiceGraph.Node(dependencyKey,
						getServiceOrNull(dependencyKey));
				graph.add(serviceNode, dependencyNode);
			}
		}
	}

	private final class SingletonServiceScope implements Scope {
		@SuppressWarnings("unchecked")
		@Override
		public <T> Provider<T> scope(final Key<T> key, final Provider<T> unscoped) {
			return new Provider<T>() {
				@Override
				public T get() {
					synchronized (SingletonServiceScope.this) {
						T instance = (T) services.get(key);
						if (instance == null) {
							instance = unscoped.get();
							services.put(key, instance);
						}
						return instance;
					}
				}
			};
		}
	}

	@Override
	protected void configure() {
		SingletonServiceScope serviceScope = new SingletonServiceScope();
		bindScope(SingletonService.class, serviceScope);
	}

	/**
	 * Creates the new ServiceGraph without  circular dependencies and intermediate nodes
	 *
	 * @param injector injector for building the graphs of objects
	 * @return created ServiceGraph
	 */
	@Provides
	@Singleton
	ServiceGraph serviceGraph(final Injector injector) {
		return new ServiceGraph() {
			@Override
			protected void onStart() {
				createGuiceGraph(injector, this);
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

}
