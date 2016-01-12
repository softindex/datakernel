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

import com.google.common.collect.HashMultimap;
import com.google.common.collect.SetMultimap;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.inject.*;
import com.google.inject.internal.MoreTypes;
import com.google.inject.spi.BindingScopingVisitor;
import com.google.inject.spi.Dependency;
import com.google.inject.spi.HasDependencies;
import io.datakernel.annotation.Nullable;
import io.datakernel.eventloop.NioEventloop;
import io.datakernel.eventloop.NioServer;
import io.datakernel.eventloop.NioService;
import org.slf4j.Logger;

import javax.sql.DataSource;
import java.io.Closeable;
import java.lang.annotation.Annotation;
import java.lang.reflect.Method;
import java.lang.reflect.Type;
import java.util.*;
import java.util.concurrent.*;

import static com.google.common.base.Preconditions.*;
import static com.google.common.collect.Sets.*;
import static org.slf4j.LoggerFactory.getLogger;

public final class BootModule extends AbstractModule {
	private static final Logger logger = getLogger(BootModule.class);

	private final Map<Class<?>, ServiceAdapter<?>> factoryMap = new LinkedHashMap<>();
	private final Map<Key<?>, ServiceAdapter<?>> keys = new LinkedHashMap<>();

	private final SetMultimap<Key<?>, Key<?>> addedDependencies = HashMultimap.create();
	private final SetMultimap<Key<?>, Key<?>> removedDependencies = HashMultimap.create();

	private final IdentityHashMap<Object, CachedService> services = new IdentityHashMap<>();
//	private final Map<Key<?>, AsyncService> singletonServices = new HashMap<>();

	private final Executor executor;

	private Map<Key<?>, Object>[] pool;
	@Nullable
	private Integer currentPoolId;

	private BootModule() {
		this.executor = new ThreadPoolExecutor(0, Integer.MAX_VALUE,
				10, TimeUnit.MILLISECONDS,
				new SynchronousQueue<Runnable>());
	}

	public static BootModule defaultInstance() {
		BootModule bootModule = new BootModule();
		bootModule.register(Service.class, ServiceAdapters.forService());
		bootModule.register(BlockingService.class, ServiceAdapters.forBlockingService());
		bootModule.register(Closeable.class, ServiceAdapters.forCloseable());
		bootModule.register(ExecutorService.class, ServiceAdapters.forExecutorService());
		bootModule.register(Timer.class, ServiceAdapters.forTimer());
		bootModule.register(DataSource.class, ServiceAdapters.forDataSource());
		bootModule.register(NioService.class, ServiceAdapters.forNioService());
		bootModule.register(NioServer.class, ServiceAdapters.forNioServer());
		bootModule.register(NioEventloop.class, ServiceAdapters.forNioEventloop());
		return bootModule;
	}

	public static BootModule instanceWithoutAdapters() {
		return new BootModule();
	}

	private static boolean isSingleton(Binding<?> binding) {
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

	private static String prettyPrintAnnotation(Annotation annotation) {
		StringBuilder sb = new StringBuilder();
		Method[] methods = annotation.annotationType().getDeclaredMethods();
		boolean first = true;
		if (methods.length != 0) {
			for (Method m : methods) {
				try {
					Object value = m.invoke(annotation);
					String attribute = (value instanceof String ? "\"" + value + "\"" : value.toString());
					String methodName = m.getName();
					if ("value".equals(methodName) && first) {
						if ((value instanceof String) && !((String) value).isEmpty()) {
							sb.append((first ? "" : ",") + attribute);
							first = false;
						}
					} else {
						sb.append((first ? "" : ",") + methodName + "=" + attribute);
						first = false;
					}
				} catch (Exception ignored) {
				}
			}
		}
		String simpleName = annotation.annotationType().getSimpleName();
		return "@" + ("NamedImpl".equals(simpleName) ? "Named" : simpleName) + (first ? "" : "(" + sb.toString() + ")");
	}

	/**
	 * Puts an instance of class and its factory to the factoryMap
	 *
	 * @param <T>     type of service
	 * @param type    key with which the specified factory is to be associated
	 * @param factory value to be associated with the specified type
	 * @return ServiceGraphModule with change
	 */
	public <T> BootModule register(Class<? extends T> type, ServiceAdapter<T> factory) {
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
	public <T> BootModule registerForSpecificKey(Key<T> key, ServiceAdapter<T> factory) {
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

	@SuppressWarnings("unchecked")
	private Service getServiceOrNull(Key<?> key, Object instance) {
		checkNotNull(instance);
		CachedService service = services.get(instance);
		if (service != null) {
			return service;
		}
		ServiceAdapter<?> serviceAdapter = keys.get(key);
		if (serviceAdapter == null) {
			Class<?> foundType = null;
			for (Class<?> type : factoryMap.keySet()) {
				if (type.isAssignableFrom(instance.getClass())) {
					foundType = type;
				}
			}
			if (foundType != null) {
				serviceAdapter = factoryMap.get(foundType);
			}
		}
		if (serviceAdapter != null) {
			Service asyncService = ((ServiceAdapter<Object>) serviceAdapter).toService(instance, executor);
			service = new CachedService(asyncService);
			services.put(instance, service);
			return service;
		}
		return null;
	}

	private void createGuiceGraph(final Injector injector, final ServiceGraph graph) {
		if (!difference(keys.keySet(), injector.getAllBindings().keySet()).isEmpty()) {
			logger.warn("Unused keys : {}", keys.keySet());
		}

		for (final Binding<?> binding : injector.getAllBindings().values()) {
			if (!isSingleton(binding) || binding.getKey().getTypeLiteral().getRawType() == ServiceGraph.class)
				continue;
			final Key<?> key = binding.getKey();
			Object instance = injector.getInstance(key);
			Service service = getServiceOrNull(key, instance);
			ServiceGraphKey serviceGraphKey = new ServiceGraphKey(key);
			graph.add(serviceGraphKey, service);
			processDependencies(serviceGraphKey, key, injector, graph);
		}

		for (int workerThreadId = 0; workerThreadId < (pool == null ? 0 : pool.length); workerThreadId++) {
			for (Map.Entry<Key<?>, Object> entry : pool[workerThreadId].entrySet()) {
				Key<?> key = entry.getKey();
				Object instance = entry.getValue();
				Service service = getServiceOrNull(key, instance);
				ServiceGraphKey serviceGraphKey = new ServiceGraphKey(key, workerThreadId);
				graph.add(serviceGraphKey, service);
				processDependencies(serviceGraphKey, key, injector, graph);
			}
		}
	}

	private void processDependencies(ServiceGraphKey serviceGraphKey, Key<?> key, Injector injector, ServiceGraph graph) {
		Binding<?> binding = injector.getBinding(key);
		if (!(binding instanceof HasDependencies))
			return;

		Set<Key<?>> dependenciesForKey = new HashSet<>();
		Set<Dependency<?>> dependencies = ((HasDependencies) binding).getDependencies();

		boolean dependsOnPool = false;
		for (Dependency<?> dependency : dependencies) {
			Key<?> dependencyKey = dependency.getKey();
			dependenciesForKey.add(dependencyKey);
			if (dependencyKey.getTypeLiteral().getRawType() == WorkerThreadsPool.class) {
				dependsOnPool = true;
			}
		}

		if (!difference(removedDependencies.get(key), dependenciesForKey).isEmpty()) {
			logger.warn("Unused removed dependencies for {} : {}", key, difference(removedDependencies.get(key), dependenciesForKey));
		}

		if (!intersection(dependenciesForKey, addedDependencies.get(key)).isEmpty()) {
			logger.warn("Unused added dependencies for {} : {}", key, intersection(dependenciesForKey, addedDependencies.get(key)));
		}

		for (Key<?> dependencyKey : union(difference(dependenciesForKey, removedDependencies.get(key)), addedDependencies.get(key))) {
			if (dependsOnPool && dependencyKey.getTypeLiteral().getRawType() == Provider.class &&
					dependencyKey.getAnnotationType() == WorkerThread.class) {
				Type actualType = ((MoreTypes.ParameterizedTypeImpl)
						dependencyKey.getTypeLiteral().getType()).getActualTypeArguments()[0];
				Key<?> actualKey = Key.get(actualType, dependencyKey.getAnnotation());
				for (int i = 0; i < pool.length; i++) {
					if (!pool[i].containsKey(actualKey)) {
						logger.warn("Not found " + actualKey + " in nio worker pool " + i);
					}
					graph.add(serviceGraphKey, new ServiceGraphKey(actualKey, i));
				}
				continue;
			}

			if (dependencyKey.getAnnotationType() == WorkerThread.class) {
				checkArgument(serviceGraphKey.workerThreadId != null, "Can only add dependency to " + dependencyKey + " from within WorkerThread, key: " + serviceGraphKey.key);
				graph.add(serviceGraphKey, new ServiceGraphKey(dependencyKey, serviceGraphKey.workerThreadId));
			} else {
				graph.add(serviceGraphKey, new ServiceGraphKey(dependencyKey));
			}
		}
	}

	@Override
	protected void configure() {
		NioWorkerScope nioNioWorkerScope = new NioWorkerScope();
		bindScope(WorkerThread.class, nioNioWorkerScope);
		bind(WorkerThreadsPool.class).toInstance(nioNioWorkerScope);
		bind(Integer.class).annotatedWith(WorkerId.class).toProvider(new Provider<Integer>() {
			@Override
			public Integer get() {
				return currentPoolId;
			}
		});
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
		ServiceGraph serviceGraph = ServiceGraph.create();
		createGuiceGraph(injector, serviceGraph);
		serviceGraph.removeIntermediateNodes();
		logger.info("Services graph: \n" + serviceGraph);
		return serviceGraph;
	}

	private static class CachedService implements Service {
		private final Service service;
		private ListenableFuture<?> startFuture;
		private ListenableFuture<?> stopFuture;

		private CachedService(Service service) {
			this.service = service;
		}

		@Override
		synchronized public ListenableFuture<?> start() {
			checkState(stopFuture == null);
			if (startFuture == null) {
				startFuture = service.start();
			}
			return startFuture;
		}

		@Override
		synchronized public ListenableFuture<?> stop() {
			checkState(startFuture != null);
			if (stopFuture == null) {
				stopFuture = service.stop();
			}
			return stopFuture;
		}
	}

	private static class ServiceGraphKey {
		private final Key<?> key;
		@Nullable
		private final Integer workerThreadId;

		public ServiceGraphKey(Key<?> key, int workerThreadId) {
			checkArgument(workerThreadId >= 0);
			this.key = checkNotNull(key);
			this.workerThreadId = workerThreadId;
		}

		public ServiceGraphKey(Key<?> key) {
			this.key = key;
			this.workerThreadId = null;
		}

		@Override
		public boolean equals(Object o) {
			if (this == o) return true;
			if (o == null || getClass() != o.getClass()) return false;

			ServiceGraphKey that = (ServiceGraphKey) o;

			if (!key.equals(that.key)) return false;
			return workerThreadId != null ? workerThreadId.equals(that.workerThreadId) : that.workerThreadId == null;

		}

		@Override
		public int hashCode() {
			int result = key.hashCode();
			result = 31 * result + (workerThreadId != null ? workerThreadId.hashCode() : 0);
			return result;
		}

		@Override
		public String toString() {
			Annotation annotation = key.getAnnotation();

			return key.getTypeLiteral() +
					(annotation != null ? " " + prettyPrintAnnotation(annotation) : "") +
					(workerThreadId != null ? " [" + workerThreadId + "]" : "");
		}
	}

	private final class NioWorkerScope implements Scope, WorkerThreadsPool {
		@Override
		public <T> Provider<T> scope(final Key<T> key, final Provider<T> unscoped) {
			return new Provider<T>() {
				@Override
				public T get() {
					checkScope(key);
					Map<Key<?>, Object> keyToInstance = pool[currentPoolId];
					@SuppressWarnings("unchecked")
					T instance = (T) keyToInstance.get(key);
					if (instance == null && !keyToInstance.containsKey(key)) {
						instance = unscoped.get();
						checkNioSingleton(key, instance);
						keyToInstance.put(key, instance);
					}
					return instance;
				}
			};
		}

		private <T> void checkScope(Key<T> key) {
			if (currentPoolId < 0 || currentPoolId > pool.length)
				// TODO (vsavchuk): try to simplify, use to Key.toString instead
				throw new RuntimeException("Could not bind " + ((key.getAnnotation() == null) ? "" :
						"@" + key.getAnnotation() + " ") + key.getTypeLiteral() + " in NioPoolScope.");
		}

		private <T> void checkNioSingleton(Key<T> key, T object) {
			for (int i = 0; i < pool.length; i++) {
				if (i == currentPoolId) continue;
				Map<Key<?>, Object> scopedObjects = pool[i];
				Object o = scopedObjects.get(key);
				if (o != null && o == object)
					// TODO (vsavchuk): use Key in error message, fix error message
					throw new IllegalStateException("Provider must returns NioSingleton object of " + object.getClass());
			}
		}

		@Override
		public <T> List<T> getPoolInstances(int size, Provider<T> itemProvider) {
			checkArgument(size > 0, "Pool size must be positive value, got %s", size);
			if (pool == null) {
				pool = new HashMap[size];
				for (int i = 0; i < size; i++) {
					pool[i] = new HashMap<>();
				}
			}
			checkArgument(pool.length == size, "Pool cannot have different size: old size = %s, new size: %s", pool.length, size);

			Integer originalPoolId = currentPoolId;
			List<T> result = new ArrayList<>(size);
			for (int i = 0; i < pool.length; i++) {
				currentPoolId = i;
				result.add(itemProvider.get());
			}
			currentPoolId = originalPoolId;
			return result;
		}

	}
}