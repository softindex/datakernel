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
import com.google.common.reflect.TypeToken;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.inject.*;
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

	private final Executor executor;

	private WorkerThreadsPoolImpl workerThreadsPool;

	private List<Listener> listeners = new ArrayList<>();

	public interface Listener {
		void onSingletonStart(Key<?> key, Object singletonInstance);

		void onSingletonStop(Key<?> key, Object singletonInstance);

		void onWorkersStart(Key<?> key, List<?> poolInstances);

		void onWorkersStop(Key<?> key, List<?> poolInstances);
	}

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

	public static BootModule newInstance() {
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

	private static boolean isWorkerThread(Key<?> binding) {
		return binding.getAnnotationType() == WorkerThread.class;
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
						if (!(value instanceof String) || !((String) value).isEmpty()) {
							sb.append(attribute);
							first = false;
						}
					} else {
						sb.append(first ? "" : ",").append(methodName).append("=").append(attribute);
						first = false;
					}
				} catch (Exception ignored) {
				}
			}
		}
		String simpleName = annotation.annotationType().getSimpleName();
		return "@" + ("NamedImpl".equals(simpleName) ? "Named" : simpleName) + (first ? "" : "(" + sb + ")");
	}

	public BootModule addListener(Listener listener) {
		listeners.add(listener);
		return this;
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

	private Service getPoolServiceOrNull(final Key<?> key, final List<?> instances) {
		final List<Service> services = new ArrayList<>();
		for (Object instance : instances) {
			Service service = getServiceOrNull(key, instance);
			if (service != null) {
				services.add(service);
			}
		}
		if (services.isEmpty())
			return null;
		return new Service() {
			@Override
			public ListenableFuture<?> start() {
				List<ListenableFuture<?>> futures = new ArrayList<>();
				for (Service service : services) {
					ListenableFuture<?> future = service.start();
					futures.add(future);
				}
				ListenableFuture<List<Object>> future = Futures.allAsList(futures);
				future.addListener(new Runnable() {
					@Override
					public void run() {
						for (Listener listener : listeners) {
							listener.onWorkersStart(key, instances);
						}
					}
				}, executor);
				return future;
			}

			@Override
			public ListenableFuture<?> stop() {
				List<ListenableFuture<?>> futures = new ArrayList<>();
				for (Service service : services) {
					ListenableFuture<?> future = service.stop();
					futures.add(future);
				}
				ListenableFuture<List<Object>> future = Futures.allAsList(futures);
				future.addListener(new Runnable() {
					@Override
					public void run() {
						for (Listener listener : listeners) {
							listener.onWorkersStop(key, instances);
						}
					}
				}, executor);
				return future;
			}
		};
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
			service = new CachedService(key, instance, asyncService);
			services.put(instance, service);
			return service;
		}
		return null;
	}

	private void createGuiceGraph(final Injector injector, final ServiceGraph graph) {
		if (!difference(keys.keySet(), injector.getAllBindings().keySet()).isEmpty()) {
			logger.warn("Unused keys : {}", keys.keySet());
		}

		Set<Key<?>> workerThreadRoots = new LinkedHashSet<>();

		for (Binding<?> binding : injector.getAllBindings().values()) {
			if (isWorkerThread(binding.getKey())) {
				workerThreadRoots.add(binding.getKey());
			}
		}

		for (Binding<?> binding : injector.getAllBindings().values()) {
			if (isWorkerThread(binding.getKey())) {
				if (binding instanceof HasDependencies) {
					for (Dependency<?> dependency : ((HasDependencies) binding).getDependencies()) {
						workerThreadRoots.remove(dependency.getKey());
					}
				}
			}
		}

		for (Binding<?> binding : injector.getAllBindings().values()) {
			if (binding.getKey().getTypeLiteral().getRawType() == ServiceGraph.class)
				continue;
			final Key<?> key = binding.getKey();
			Service service;
			if (isSingleton(binding)) {
				Object instance = injector.getInstance(key);
				service = getServiceOrNull(key, instance);
			} else if (isWorkerThread(key)) {
				List<?> instances = workerThreadsPool.getPoolInstances(key);
				service = getPoolServiceOrNull(key, instances);
			} else
				continue;
			graph.add(key, service);
			processDependencies(key, injector, graph, workerThreadRoots);
		}

	}

	private void processDependencies(Key<?> key, Injector injector, ServiceGraph graph, Set<Key<?>> workerThreadRoots) {
		Binding<?> binding = injector.getBinding(key);
		if (!(binding instanceof HasDependencies))
			return;

		Set<Key<?>> dependencies = new HashSet<>();
		for (Dependency<?> dependency : ((HasDependencies) binding).getDependencies()) {
			dependencies.add(dependency.getKey());
		}

		if (!difference(removedDependencies.get(key), dependencies).isEmpty()) {
			logger.warn("Unused removed dependencies for {} : {}", key, difference(removedDependencies.get(key), dependencies));
		}

		if (!intersection(dependencies, addedDependencies.get(key)).isEmpty()) {
			logger.warn("Unused added dependencies for {} : {}", key, intersection(dependencies, addedDependencies.get(key)));
		}

		for (Key<?> dependencyKey : union(difference(dependencies, removedDependencies.get(key)), addedDependencies.get(key))) {
			if (dependencyKey.getTypeLiteral().getRawType() == WorkerThreadsPool.class) {
				graph.add(key, workerThreadRoots);
			}
			graph.add(key, dependencyKey);
		}
	}

	@Override
	protected void configure() {
		workerThreadsPool = new WorkerThreadsPoolImpl();
		requestInjection(workerThreadsPool);
		bindScope(WorkerThread.class, workerThreadsPool);
		bind(Integer.class).annotatedWith(WorkerId.class).toProvider(new Provider<Integer>() {
			@Override
			public Integer get() {
				return workerThreadsPool.currentPoolId;
			}
		});
	}

	@Provides
	WorkerThreadsPool workerThreadsPool() {
		checkArgument(workerThreadsPool.poolSize != null, "Pool size must be provided, as Integer annotated with %s", WorkerThreadsPoolSize.class);
		return workerThreadsPool;
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
		ServiceGraph serviceGraph = new ServiceGraph() {
			@Override
			protected String nodeToString(Object node) {
				Key<?> key = (Key<?>) node;
				Annotation annotation = key.getAnnotation();
				return key.getTypeLiteral() +
						(annotation != null ? " " + prettyPrintAnnotation(annotation) : "");
			}
		};
		createGuiceGraph(injector, serviceGraph);
		serviceGraph.removeIntermediateNodes();
		logger.info("Services graph: \n" + serviceGraph);
		return serviceGraph;
	}

	private class CachedService implements Service {
		private final Key<Object> key;
		private final Object instance;
		private final Service service;
		private ListenableFuture<?> startFuture;
		private ListenableFuture<?> stopFuture;

		private CachedService(Key<?> key, Object instance, Service service) {
			this.key = (Key<Object>) key;
			this.instance = instance;
			this.service = service;
		}

		@Override
		synchronized public ListenableFuture<?> start() {
			checkState(stopFuture == null);
			if (startFuture == null) {
				startFuture = service.start();
			}
			if (!isWorkerThread(key)) {
				startFuture.addListener(new Runnable() {
					@Override
					public void run() {
						for (Listener listener : listeners) {
							listener.onSingletonStart(key, instance);
						}
					}
				}, executor);
			}
			return startFuture;
		}

		@Override
		synchronized public ListenableFuture<?> stop() {
			checkState(startFuture != null);
			if (stopFuture == null) {
				stopFuture = service.stop();
			}
			if (!isWorkerThread(key)) {
				startFuture.addListener(new Runnable() {
					@Override
					public void run() {
						for (Listener listener : listeners) {
							listener.onSingletonStop(key, instance);
						}
					}
				}, executor);
			}
			return stopFuture;
		}
	}

	private final class WorkerThreadsPoolImpl implements Scope, WorkerThreadsPool {
		private final Map<Key<?>, Object[]> pool = new HashMap<>();

		@Inject(optional = true)
		@WorkerThreadsPoolSize
		Integer poolSize;

		@Inject
		Injector injector;

		@Nullable
		Integer currentPoolId;

		@Override
		public <T> Provider<T> scope(final Key<T> key, final Provider<T> unscoped) {
			return new Provider<T>() {
				@SuppressWarnings("unchecked")
				@Override
				public T get() {
					checkState(currentPoolId != null && poolSize != null,
							"To create %s use %s", key, WorkerThreadsPool.class.getSimpleName());
					T[] instances = (T[]) pool.get(key);
					if (instances == null) {
						instances = (T[]) new Object[poolSize];
						pool.put(key, instances);
					}
					T instance = instances[currentPoolId];
					if (instance == null) {
						instance = unscoped.get();
						instances[currentPoolId] = instance;
					}
					return instance;
				}
			};
		}

		@Override
		public <T> List<T> getPoolInstances(Class<T> type) {
			return getPoolInstances(Key.get(type, WorkerThread.class));
		}

		@SuppressWarnings("unchecked")
		@Override
		public <T> List<T> getPoolInstances(TypeToken<T> type) {
			return getPoolInstances((Key<T>) Key.get(type.getType(), WorkerThread.class));
		}

		@Override
		public <T> List<T> getPoolInstances(Class<T> type, String named) {
			return getPoolInstances(Key.get(type, new WorkerThreadAnnotation(named)));
		}

		@SuppressWarnings("unchecked")
		@Override
		public <T> List<T> getPoolInstances(TypeToken<T> type, String named) {
			return getPoolInstances((Key<T>) Key.get(type.getType(), new WorkerThreadAnnotation(named)));
		}

		private <T> List<T> getPoolInstances(Key<T> key) {
			checkArgument(isWorkerThread(key));
			Integer originalPoolId = currentPoolId;
			List<T> result = new ArrayList<>();
			for (int i = 0; i < poolSize; i++) {
				currentPoolId = i;
				result.add(injector.getInstance(key));
			}
			currentPoolId = originalPoolId;
			return result;
		}
	}

	@SuppressWarnings("ClassExplicitlyAnnotation")
	private static final class WorkerThreadAnnotation implements WorkerThread {
		final String value;

		WorkerThreadAnnotation(String value) {
			this.value = checkNotNull(value);
		}

		@Override
		public String value() {
			return value;
		}

		@Override
		public Class<? extends Annotation> annotationType() {
			return WorkerThread.class;
		}

		@Override
		public boolean equals(Object o) {
			if (!(o instanceof WorkerThread)) return false;

			WorkerThread that = (WorkerThread) o;
			return value.equals(that.value());

		}

		@Override
		public int hashCode() {
			return (127 * "value".hashCode()) ^ value.hashCode();
		}

		public String toString() {
			return "@" + WorkerThread.class.getName() + "(value=" + value + ")";
		}

	}

}