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

package io.datakernel.guice.boot;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.SetMultimap;
import com.google.inject.*;
import com.google.inject.internal.MoreTypes;
import com.google.inject.spi.BindingScopingVisitor;
import com.google.inject.spi.Dependency;
import com.google.inject.spi.HasDependencies;
import io.datakernel.eventloop.NioEventloop;
import io.datakernel.eventloop.NioServer;
import io.datakernel.eventloop.NioService;
import io.datakernel.guice.WorkerId;
import io.datakernel.guice.WorkerThread;
import io.datakernel.guice.WorkerThreadsPool;
import io.datakernel.service.AsyncService;
import io.datakernel.service.AsyncServices;
import io.datakernel.service.Service;
import io.datakernel.service.ServiceGraph;
import org.slf4j.Logger;

import javax.sql.DataSource;
import java.io.Closeable;
import java.lang.annotation.Annotation;
import java.lang.reflect.Type;
import java.util.*;
import java.util.concurrent.*;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.collect.Sets.*;
import static org.slf4j.LoggerFactory.getLogger;

public final class BootModule extends AbstractModule {
	private static final Logger logger = getLogger(BootModule.class);

	private final Map<Class<?>, AsyncServiceAdapter<?>> factoryMap = new LinkedHashMap<>();
	private final Map<Key<?>, AsyncServiceAdapter<?>> keys = new LinkedHashMap<>();

	private final SetMultimap<Key<?>, Key<?>> addedDependencies = HashMultimap.create();
	private final SetMultimap<Key<?>, Key<?>> removedDependencies = HashMultimap.create();

	private final IdentityHashMap<Object, AsyncService> workerThreadServices = new IdentityHashMap<>();
	private final Map<Key<?>, AsyncService> singletonServices = new HashMap<>();

	private final Executor executor;

	private List<Map<Key<?>, Object>> pool;
	private Map<Key<?>, List<BootModule.KeyInPool>> mapKeys;
	private int currentPool = -1;

	private BootModule() {
		this.executor = new ThreadPoolExecutor(0, Integer.MAX_VALUE,
				10, TimeUnit.MILLISECONDS,
				new SynchronousQueue<Runnable>());
	}

	public static BootModule defaultInstance() {
		BootModule bootModule = new BootModule();
		bootModule.register(AsyncService.class, AsyncServiceAdapters.forAsyncService());
		bootModule.register(Service.class, AsyncServiceAdapters.forBlockingService());
		bootModule.register(Closeable.class, AsyncServiceAdapters.forCloseable());
		bootModule.register(ExecutorService.class, AsyncServiceAdapters.forExecutorService());
		bootModule.register(Timer.class, AsyncServiceAdapters.forTimer());
		bootModule.register(DataSource.class, AsyncServiceAdapters.forDataSource());
		bootModule.register(NioService.class, AsyncServiceAdapters.forNioService());
		bootModule.register(NioServer.class, AsyncServiceAdapters.forNioServer());
		bootModule.register(NioEventloop.class, AsyncServiceAdapters.forNioEventloop());
		return bootModule;
	}

	public static BootModule instanceWithoutAdapters() {
		return new BootModule();
	}

	/**
	 * Puts an instance of class and its factory to the factoryMap
	 *
	 * @param <T>     type of service
	 * @param type    key with which the specified factory is to be associated
	 * @param factory value to be associated with the specified type
	 * @return ServiceGraphModule with change
	 */
	public <T> BootModule register(Class<? extends T> type, AsyncServiceAdapter<T> factory) {
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
	public <T> BootModule registerForSpecificKey(Key<T> key, AsyncServiceAdapter<T> factory) {
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
	public BootModule addDependency(Key<?> key, Key<?> keyDependency) {
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
	public BootModule removeDependency(Key<?> key, Key<?> keyDependency) {
		removedDependencies.put(key, keyDependency);
		return this;
	}

	private interface AddDependence {
		void add(Key<?> dependencyKey);
	}

	private void processDependencies(ServiceGraph.Node serviceNode, Key<?> key, Injector injector, ServiceGraph graph,
	                                 AddDependence addDependence) {
		Binding<?> binding = injector.getBinding(key);
		if (binding instanceof HasDependencies) {
			Set<Key<?>> dependenciesForKey = new HashSet<>();
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

	@SuppressWarnings("unchecked")
	private AsyncService getServiceFromNioScopeInstanceOrNull(Key<?> key, Object instance) {
		checkNotNull(instance);
		AsyncService asyncService = workerThreadServices.get(instance);
		if (asyncService != null) {
			return asyncService;
		}
		AsyncServiceAdapter<?> factoryForKey = keys.get(key);
		if (factoryForKey != null) {
			asyncService = ((AsyncServiceAdapter<Object>) factoryForKey).toService(instance, executor);
			workerThreadServices.put(instance, asyncService);
			return asyncService;
		}
		Class<?> foundType = null;
		for (Class<?> type : factoryMap.keySet()) {
			if (type.isAssignableFrom(instance.getClass())) {
				foundType = type;
			}
		}
		if (foundType == null)
			return null;
		AsyncServiceAdapter<?> asyncServiceAdapter = factoryMap.get(foundType);
		asyncService = ((AsyncServiceAdapter<Object>) asyncServiceAdapter).toService(instance, executor);
		checkNotNull(asyncService);
		workerThreadServices.put(instance, asyncService);
		return asyncService;
	}

	private ServiceGraph.Node nodeFromNioScope(KeyInPool keyInPool, Object instance) {
		return new ServiceGraph.Node(keyInPool, getServiceFromNioScopeInstanceOrNull(keyInPool.key, instance));
	}

	private ServiceGraph.Node nodeFromNioScope(Key<?> key, int poolIndex, Object instance) {
		return nodeFromNioScope(new KeyInPool(key, poolIndex), instance);
	}

	private boolean isSingleton(Binding<?> binding) {
		return binding.acceptScopingVisitor(new BindingScopingVisitor<Boolean>() {
			public Boolean visitNoScoping() {
				return false;
			}

			public Boolean visitScopeAnnotation(Class<? extends Annotation> visitedAnnotation) {
				return visitedAnnotation.equals(Singleton.class);
			}

			public Boolean visitScope(Scope visitedScope) {
				return visitedScope.equals(Scopes.SINGLETON);
			}

			public Boolean visitEagerSingleton() {
				return true;
			}
		});
	}

	@SuppressWarnings("unchecked")
	private AsyncService getServiceOrNull(Key<?> key, Injector injector) {
		if (singletonServices.containsKey(key)) {
			return singletonServices.get(key);
		}
		Binding<?> binding = injector.getBinding(key);

		if (!isSingleton(binding)) return null;

		Object object = injector.getInstance(key);

		AsyncServiceAdapter<?> factoryForKey = keys.get(key);
		if (factoryForKey != null) {
			checkNotNull(object, "SingletonService object is not instantiated for " + key);
			AsyncService service = ((AsyncServiceAdapter<Object>) factoryForKey).toService(object, executor);
			singletonServices.put(key, service);
			return checkNotNull(service);
		}
		if (object == null)
			return null;
		for (Class<?> type : factoryMap.keySet()) {
			if (type.isAssignableFrom(object.getClass())) {
				AsyncServiceAdapter<?> asyncServiceAdapter = factoryMap.get(type);
				AsyncService service = ((AsyncServiceAdapter<Object>) asyncServiceAdapter).toService(object, executor);
				singletonServices.put(key, service);
				return checkNotNull(service);
			}
		}
		if (binding instanceof HasDependencies) {
			Set<Dependency<?>> dependencies = ((HasDependencies) binding).getDependencies();
			for (Dependency<?> dependency : dependencies) {
				if (dependency.getKey().equals(Key.get(WorkerThreadsPool.class))) {
					AsyncService asyncService = AsyncServices.immediateService();
					singletonServices.put(key, asyncService);
					return asyncService;
				}
			}
		}
		return null;
	}

	private ServiceGraph.Node nodeFromService(Key<?> key, Injector injector) {
		return new ServiceGraph.Node(key, getServiceOrNull(key, injector));
	}

	private void createGuiceGraph(final Injector injector, final NioWorkerScope nioNioWorkerScope, final ServiceGraph graph) {
		if (!difference(keys.keySet(), injector.getAllBindings().keySet()).isEmpty()) {
			logger.warn("Unused keys : {}", keys.keySet());
		}

		for (final Key<?> key : injector.getAllBindings().keySet()) {
			final ServiceGraph.Node serviceNode = nodeFromService(key, injector);

			graph.add(serviceNode);
			processDependencies(serviceNode, key, injector, graph, new AddDependence() {
				@Override
				public void add(Key<?> dependencyKey) {
					final List<Map<Key<?>, Object>> pool = nioNioWorkerScope.getPool();
					final Map<Key<?>, List<KeyInPool>> mapKeys = nioNioWorkerScope.getMapKeys();

					if (dependencyKey.equals(Key.get(WorkerThreadsPool.class))) {
						Set<Dependency<?>> dependencies = ((HasDependencies) injector.getBinding(key)).getDependencies();
						for (Dependency<?> dependency : dependencies) {
							Key<?> dependencyForKey = dependency.getKey();

							if (dependencyForKey.getAnnotationType() != null
									&& dependencyForKey.getTypeLiteral().getRawType().equals(Provider.class)
									&& dependencyForKey.getAnnotationType().equals(WorkerThread.class)) {

								Type actualType = ((MoreTypes.ParameterizedTypeImpl)
										dependencyForKey.getTypeLiteral().getType()).getActualTypeArguments()[0];

								if (!mapKeys.containsKey(Key.get(actualType, dependencyForKey.getAnnotation()))) {
									logger.warn("Not found "
											+ Key.get(actualType, dependencyForKey.getAnnotation())
											+ " in nio worker pool");
									continue;
								}
								for (KeyInPool actualKeyInPool : mapKeys.get(Key.get(actualType, dependencyForKey.getAnnotation()))) {
									ServiceGraph.Node dependencyNode = nodeFromNioScope(actualKeyInPool,
											pool.get(actualKeyInPool.index).get(actualKeyInPool.key));
									graph.add(serviceNode, dependencyNode);
								}
							}
						}
					}
				}
			});
		}

		for (int poolIndex = 0; poolIndex < nioNioWorkerScope.getPool().size(); poolIndex++) {
			Map<Key<?>, Object> nioScopeMap = nioNioWorkerScope.getPool().get(poolIndex);
			createNioScopeGraph(nioScopeMap, injector, graph, poolIndex);
		}
	}

	private void createNioScopeGraph(final Map<Key<?>, Object> map, Injector injector, final ServiceGraph graph, final int poolIndex) {
		for (Key<?> key : map.keySet()) {
			Object nioScopeObject = map.get(key);
			final ServiceGraph.Node serviceNode = nodeFromNioScope(key, poolIndex, nioScopeObject);

			graph.add(serviceNode);
			processDependencies(serviceNode, key, injector, graph, new AddDependence() {

				@Override
				public void add(Key<?> dependencyKey) {
					if (map.containsKey(dependencyKey)) {
						Object nioScopeDependencyObject = map.get(dependencyKey);
						ServiceGraph.Node dependencyNode = nodeFromNioScope(dependencyKey, poolIndex, nioScopeDependencyObject);
						graph.add(serviceNode, dependencyNode);
					}
				}
			});
		}
	}

	@Override
	protected void configure() {
		NioWorkerScope nioNioWorkerScope = new NioWorkerScope();
		bindScope(WorkerThread.class, nioNioWorkerScope);
		bind(NioWorkerScope.class).toInstance(nioNioWorkerScope);
		bind(WorkerThreadsPool.class).toInstance(nioNioWorkerScope);
		bind(Integer.class).annotatedWith(WorkerId.class).toProvider(nioNioWorkerScope.getNumberScopeProvider());
	}

	/**
	 * Creates the new ServiceGraph without  circular dependencies and intermediate nodes
	 *
	 * @param injector injector for building the graphs of objects
	 * @return created ServiceGraph
	 */
	@Provides
	@Singleton
	ServiceGraph serviceGraph(final Injector injector, final NioWorkerScope nioNioWorkerScope) {
		return new ServiceGraph() {
			@Override
			protected void onStart() {
				createGuiceGraph(injector, nioNioWorkerScope, this);
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

	private final class NioWorkerScope implements Scope, WorkerThreadsPool {
		@Override
		public <T> Provider<T> scope(final Key<T> key, final Provider<T> unscoped) {
			return new Provider<T>() {
				@Override
				public T get() {
					checkScope(key);
					Map<Key<?>, Object> scopedObjects = pool.get(currentPool);
					@SuppressWarnings("unchecked")
					T current = (T) scopedObjects.get(key);
					if (current == null && !scopedObjects.containsKey(key)) {
						current = unscoped.get();
						checkNioSingleton(key, current);
						scopedObjects.put(key, current);
						if (!mapKeys.containsKey(key))
							mapKeys.put(key, new ArrayList<BootModule.KeyInPool>());
						mapKeys.get(key).add(new BootModule.KeyInPool(key, currentPool));
					}
					return current;
				}
			};
		}

		private final Provider<Integer> numberProvider = new Provider<Integer>() {
			@Override
			public Integer get() {
				return getCurrentPool();
			}
		};

		private int getCurrentPool() {
			return currentPool;
		}

		public Provider<Integer> getNumberScopeProvider() {
			return numberProvider;
		}

		private List<Map<Key<?>, Object>> initPool(int size) {
			checkArgument(size > 0, "size must be positive value, got %s", size);
			List<Map<Key<?>, Object>> pool = new ArrayList<>(size);
			for (int i = 0; i < size; i++) {
				pool.add(new HashMap<Key<?>, Object>());
			}
			return pool;
		}

		private <T> void checkScope(Key<T> key) {
			if (currentPool < 0 || currentPool > pool.size())
				throw new RuntimeException("Could not bind " + ((key.getAnnotation() == null) ? "" :
						"@" + key.getAnnotation() + " ") + key.getTypeLiteral() + " in NioPoolScope.");
		}

		private <T> void checkNioSingleton(Key<T> key, T object) {
			for (int i = 0; i < pool.size(); i++) {
				if (i == currentPool) continue;
				Map<Key<?>, Object> scopedObjects = pool.get(i);
				Object o = scopedObjects.get(key);
				if (o != null && o == object)
					throw new IllegalStateException("Provider must returns NioSingleton object of " + object.getClass());
			}
		}

		@Override
		public <T> List<T> getPoolInstances(int size, Provider<T> itemProvider) {
			if (pool == null) {
				pool = initPool(size);
				mapKeys = new HashMap<>();
			} else {
				if (pool.size() != size) {
					throw new IllegalArgumentException("Pool cannot have different size: old size = " + pool.size() + ", new size: " + size);
				}
			}

			int safeCurrentPool = currentPool;
			if (pool == null)
				throw new IllegalStateException("Scope is not initialized");
			List<T> result = new ArrayList<>(pool.size());
			for (int i = 0; i < pool.size(); i++) {
				currentPool = i;
				result.add(itemProvider.get());
			}
			currentPool = safeCurrentPool;
			return result;
		}

		private List<Map<Key<?>, Object>> getPool() {
			if (pool == null)
				return Collections.emptyList();
			else
				return pool;
		}

		private Map<Key<?>, List<BootModule.KeyInPool>> getMapKeys() {
			if (mapKeys == null) {
				return Collections.emptyMap();
			} else {
				return mapKeys;
			}
		}
	}

	private static class KeyInPool {
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
			Annotation annotation = key.getAnnotation();
			return key.getTypeLiteral() + " " + index +
					(annotation != null ? " " + annotation : "");
		}
	}
}