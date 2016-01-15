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

import com.google.common.base.Throwables;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.SetMultimap;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import com.google.inject.*;
import com.google.inject.matcher.AbstractMatcher;
import com.google.inject.spi.*;
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
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.Sets.*;
import static com.google.common.util.concurrent.MoreExecutors.directExecutor;
import static org.slf4j.LoggerFactory.getLogger;

public final class BootModule extends AbstractModule {
	private static final Logger logger = getLogger(BootModule.class);

	private final Map<Class<?>, ServiceAdapter<?>> factoryMap = new LinkedHashMap<>();
	private final Map<Key<?>, ServiceAdapter<?>> keys = new LinkedHashMap<>();

	private final SetMultimap<Key<?>, Key<?>> addedDependencies = HashMultimap.create();
	private final SetMultimap<Key<?>, Key<?>> removedDependencies = HashMultimap.create();

	private final SetMultimap<Key<?>, Key<?>> workerDependencies = HashMultimap.create();

	private final IdentityHashMap<Object, CachedService> services = new IdentityHashMap<>();

	private final Executor executor;

	private WorkerPoolScope workerPoolScope;

	private List<Listener> listeners = new ArrayList<>();

	private ServiceGraph serviceGraph;

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

	private static boolean isWorker(Key<?> binding) {
		return binding.getAnnotation() instanceof Worker;
	}

	private static String prettyPrintAnnotation(Annotation annotation) {
		StringBuilder sb = new StringBuilder();
		Method[] methods = annotation.annotationType().getDeclaredMethods();
		boolean first = true;
		if (methods.length != 0) {
			for (Method m : methods) {
				try {
					Object value = m.invoke(annotation);
					if (value.equals(m.getDefaultValue()))
						continue;
					String valueStr = (value instanceof String ? "\"" + value + "\"" : value.toString());
					String methodName = m.getName();
					if ("value".equals(methodName) && first) {
						sb.append(valueStr);
						first = false;
					} else {
						sb.append(first ? "" : ",").append(methodName).append("=").append(valueStr);
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
		boolean found = false;
		for (Object instance : instances) {
			Service service = getServiceOrNull(key, instance);
			services.add(service);
			if (service != null) {
				found = true;
			}
		}
		if (!found)
			return null;
		return new Service() {
			@Override
			public ListenableFuture<?> start() {
				List<ListenableFuture<?>> futures = new ArrayList<>();
				for (Service service : services) {
					futures.add(service != null ? service.start() : null);
				}
				ListenableFuture<?> future = combineFutures(futures, directExecutor());
				future.addListener(new Runnable() {
					@Override
					public void run() {
						for (Listener listener : listeners) {
							listener.onWorkersStart(key, instances);
						}
					}
				}, directExecutor());
				return future;
			}

			@Override
			public ListenableFuture<?> stop() {
				List<ListenableFuture<?>> futures = new ArrayList<>();
				for (Service service : services) {
					futures.add(service != null ? service.stop() : null);
				}
				ListenableFuture<?> future = combineFutures(futures, directExecutor());
				future.addListener(new Runnable() {
					@Override
					public void run() {
						for (Listener listener : listeners) {
							listener.onWorkersStop(key, instances);
						}
					}
				}, directExecutor());
				return future;
			}
		};
	}

	private static ListenableFuture<?> combineFutures(List<ListenableFuture<?>> futures, final Executor executor) {
		final SettableFuture<?> resultFuture = SettableFuture.create();
		final AtomicInteger count = new AtomicInteger(futures.size());
		final AtomicReference<Throwable> exception = new AtomicReference<>();
		for (ListenableFuture<?> future : futures) {
			final ListenableFuture<?> finalFuture = future != null ? future : Futures.immediateFuture(null);
			finalFuture.addListener(new Runnable() {
				@Override
				public void run() {
					try {
						finalFuture.get();
					} catch (InterruptedException | ExecutionException e) {
						exception.set(Throwables.getRootCause(e));
					}
					if (count.decrementAndGet() == 0) {
						if (exception.get() != null)
							resultFuture.setException(exception.get());
						else
							resultFuture.set(null);
					}
				}
			}, executor);
		}
		return resultFuture;
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
			logger.warn("Unused services : {}", difference(keys.keySet(), injector.getAllBindings().keySet()));
		}

		for (Binding<?> binding : injector.getAllBindings().values()) {
			Key<?> key = binding.getKey();
			if (isSingleton(binding)) {
				Object instance = injector.getInstance(key);
				Service service = getServiceOrNull(key, instance);
				graph.add(key, service);
			}
		}

		for (Binding<?> binding : injector.getAllBindings().values()) {
			Key<?> key = binding.getKey();
			if (isWorker(key)) {
				if (workerDependencies.values().contains(key)) {
					List<?> instances = workerPoolScope.getInstances(key);
					Service service = getPoolServiceOrNull(key, instances);
					graph.add(key, service);
				} else {
					logger.warn("Unused key: {}", key);
				}
			}
		}

		for (Binding<?> binding : injector.getAllBindings().values()) {
			processDependencies(binding.getKey(), injector, graph);
		}
	}

	private void processDependencies(Key<?> key, Injector injector, ServiceGraph graph) {
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

		for (Key<?> dependencyKey : difference(union(union(dependencies, workerDependencies.get(key)),
				addedDependencies.get(key)), removedDependencies.get(key))) {
			graph.add(key, dependencyKey);
		}
	}

	@Override
	protected void configure() {
		final Provider<Injector> injectorProvider = getProvider(Injector.class);
		bindListener(new AbstractMatcher<Binding<?>>() {
			@Override
			public boolean matches(Binding<?> binding) {
				return binding.getKey().getTypeLiteral().getRawType() == WorkerPools.class;
			}
		}, new ProvisionListener() {
			@Override
			public <T> void onProvision(ProvisionInvocation<T> provision) {
				WorkerPools workerPools = (WorkerPools) provision.provision();
				workerPools.injector = injectorProvider.get();
			}
		});

		bindListener(new AbstractMatcher<Binding<?>>() {
			@Override
			public boolean matches(Binding<?> binding) {
				return binding.getKey().getAnnotationType() == Worker.class;
			}
		}, new ProvisionListener() {
			@Override
			public <T> void onProvision(ProvisionInvocation<T> provision) {
				provision.provision();
				List<DependencyAndSource> chain = provision.getDependencyChain();
				if (chain.size() >= 2) {
					Key<?> key = chain.get(chain.size() - 2).getDependency().getKey();
					Key<T> dependencyKey = provision.getBinding().getKey();
					if (isWorker(dependencyKey) && key.getTypeLiteral().getRawType() != ServiceGraph.class) {
						workerDependencies.put(key, dependencyKey);
					}
				}
			}
		});

		workerPoolScope = new WorkerPoolScope();
		requestInjection(workerPoolScope);
		bindScope(Worker.class, workerPoolScope);
		bind(Integer.class).annotatedWith(WorkerId.class).toProvider(new Provider<Integer>() {
			@Override
			public Integer get() {
				return null;
			}
		}).in(Worker.class);
	}

	/**
	 * Creates the new ServiceGraph without  circular dependencies and intermediate nodes
	 *
	 * @param injector injector for building the graphs of objects
	 * @return created ServiceGraph
	 */
	@Provides
	synchronized ServiceGraph serviceGraph(final Injector injector) {
		if (serviceGraph == null) {
			serviceGraph = new ServiceGraph() {
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
		}
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
			if (!isWorker(key)) {
				startFuture.addListener(new Runnable() {
					@Override
					public void run() {
						for (Listener listener : listeners) {
							listener.onSingletonStart(key, instance);
						}
					}
				}, directExecutor());
			}
			return startFuture;
		}

		@Override
		synchronized public ListenableFuture<?> stop() {
			checkState(startFuture != null);
			if (stopFuture == null) {
				stopFuture = service.stop();
			}
			if (!isWorker(key)) {
				startFuture.addListener(new Runnable() {
					@Override
					public void run() {
						for (Listener listener : listeners) {
							listener.onSingletonStop(key, instance);
						}
					}
				}, directExecutor());
			}
			return stopFuture;
		}
	}

}