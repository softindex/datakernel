/*
 * Copyright (C) 2015-2018 SoftIndex LLC.
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

import io.datakernel.di.Optional;
import io.datakernel.di.*;
import io.datakernel.di.module.AbstractModule;
import io.datakernel.di.module.Provides;
import io.datakernel.di.util.ScopedValue;
import io.datakernel.di.util.Trie;
import io.datakernel.eventloop.Eventloop;
import io.datakernel.eventloop.EventloopServer;
import io.datakernel.eventloop.EventloopService;
import io.datakernel.logger.LoggerFactory;
import io.datakernel.net.BlockingSocketServer;
import io.datakernel.util.Initializable;
import io.datakernel.util.Initializer;
import io.datakernel.util.TypeT;
import io.datakernel.worker.Worker;
import io.datakernel.worker.WorkerPool;
import io.datakernel.worker.WorkerPoolModule;
import io.datakernel.worker.WorkerPools;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import javax.sql.DataSource;
import java.io.Closeable;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.logging.Level;
import java.util.logging.Logger;

import static io.datakernel.service.ServiceAdapters.*;
import static io.datakernel.util.CollectionUtils.difference;
import static io.datakernel.util.CollectionUtils.intersection;
import static io.datakernel.util.Preconditions.checkNotNull;
import static io.datakernel.util.Preconditions.checkState;
import static java.util.Collections.*;
import static java.util.concurrent.CompletableFuture.completedFuture;
import static java.util.stream.Collectors.*;

/**
 * Builds dependency graph of {@code Service} objects based on Guice's object
 * graph. Service graph module is capable to start services concurrently.
 * <p>
 * Consider some lifecycle details of this module:
 * <ul>
 * <li>
 * Put all objects from the graph which can be treated as
 * {@link Service} instances.
 * </li>
 * <li>
 * Starts services concurrently starting at leaf graph nodes (independent
 * services) and ending with root nodes.
 * </li>
 * <li>
 * Stop services starting from root and ending with independent services.
 * </li>
 * </ul>
 * <p>
 * An ability to use {@link ServiceAdapter} objects allows to create a service
 * from any object by providing it's {@link ServiceAdapter} and registering
 * it in {@code ServiceGraphModule}. Take a look at {@link ServiceAdapters},
 * which has a lot of implemented adapters. Its necessarily to annotate your
 * object provider with {@link Worker @Worker} or Singleton
 * annotation.
 * <p>
 * An application terminates if a circular dependency found.
 */
public final class ServiceGraphModule extends AbstractModule implements Initializable<ServiceGraphModule> {
	private static final Logger logger = LoggerFactory.getLogger(ServiceGraphModule.class.getName());

	private final Map<Class<?>, ServiceAdapter<?>> registeredServiceAdapters = new LinkedHashMap<>();
	private final Set<Key<?>> excludedKeys = new LinkedHashSet<>();
	private final Map<Key<?>, ServiceAdapter<?>> keys = new LinkedHashMap<>();

	private final Map<Key<?>, Set<Key<?>>> addedDependencies = new HashMap<>();
	private final Map<Key<?>, Set<Key<?>>> removedDependencies = new HashMap<>();

	private final Executor executor;

	private Initializer<ServiceGraph> initializer = Initializer.empty();

	private ServiceGraphModule() {
		this.executor = new ThreadPoolExecutor(0, Integer.MAX_VALUE,
				10, TimeUnit.MILLISECONDS,
				new SynchronousQueue<>());
	}

	/**
	 * Creates a service graph with default configuration, which is able to
	 * handle {@code Service, BlockingService, Closeable, ExecutorService,
	 * Timer, DataSource, EventloopService, EventloopServer} and
	 * {@code Eventloop} as services.
	 *
	 * @return default service graph
	 */
	public static ServiceGraphModule defaultInstance() {
		return newInstance()
				.register(Service.class, forService())
				.register(BlockingService.class, forBlockingService())
				.register(BlockingSocketServer.class, forBlockingSocketServer())
				.register(Closeable.class, forCloseable())
				.register(ExecutorService.class, forExecutorService())
				.register(Timer.class, forTimer())
				.register(DataSource.class, forDataSource())
				.register(EventloopService.class, forEventloopService())
				.register(EventloopServer.class, forEventloopServer())
				.register(Eventloop.class, forEventloop());
	}

	@Override
	protected void configure() {
		install(new WorkerPoolModule());
	}

	public static ServiceGraphModule newInstance() {
		return new ServiceGraphModule();
	}

	/**
	 * Puts an instance of class and its factory to the factoryMap
	 *
	 * @param <T>     type of service
	 * @param type    key with which the specified factory is to be associated
	 * @param factory value to be associated with the specified type
	 * @return ServiceGraphModule with change
	 */
	public <T> ServiceGraphModule register(Class<? extends T> type, ServiceAdapter<T> factory) {
		registeredServiceAdapters.put(type, factory);
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
	public <T> ServiceGraphModule registerForSpecificKey(Key<T> key, ServiceAdapter<T> factory) {
		keys.put(key, factory);
		return this;
	}

	public <T> ServiceGraphModule excludeSpecificKey(Key<T> key) {
		excludedKeys.add(key);
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
		addedDependencies.computeIfAbsent(key, key1 -> new HashSet<>()).add(keyDependency);
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
		removedDependencies.computeIfAbsent(key, key1 -> new HashSet<>()).add(keyDependency);
		return this;
	}

	public ServiceGraphModule withInitializer(Initializer<ServiceGraph> initializer) {
		this.initializer = initializer;
		return this;
	}

	private static final class ServiceKey implements ServiceGraph.Key<Object> {
		@NotNull
		private final Key<?> key;
		private final int workerPoolId;

		private ServiceKey(@NotNull Key<?> key) {
			this.key = key;
			this.workerPoolId = 0;
		}

		private ServiceKey(@NotNull Key<?> key, int id) {
			this.key = key;
			this.workerPoolId = id;
		}

		@NotNull
		public Key<?> getKey() {
			return key;
		}

		public boolean isWorker() {
			return workerPoolId != 0;
		}

		public int getWorkerPoolId() {
			return workerPoolId;
		}

		@NotNull
		@Override
		public TypeT<Object> getTypeT() {
			return TypeT.ofType(key.getType());
		}

		@Nullable
		@Override
		public Name getName() {
			return key.getName();
		}

		@Override
		public boolean equals(Object o) {
			if (this == o) {
				return true;
			}
			if (o == null || getClass() != o.getClass()) {
				return false;
			}
			ServiceKey other = (ServiceKey) o;
			return workerPoolId == other.workerPoolId &&
					key.equals(other.key);
		}

		@Override
		public int hashCode() {
			return Objects.hash(key, workerPoolId);
		}

		@Override
		public String toString() {
			return key.toString() + (workerPoolId == 0 ? "" : ":" + workerPoolId);
		}
	}

	@Provides
	ServiceGraph serviceGraph(Injector injector, @Optional Set<Initializer<ServiceGraphModule>> initializers) {
		if (initializers != null) initializers.forEach(initializer -> initializer.accept(this));
		WorkerPools workerPools = injector.peekInstance(WorkerPools.class);
		List<WorkerPool> pools = workerPools != null ? workerPools.getWorkerPools() : emptyList();
		Map<ServiceKey, List<?>> instances = new HashMap<>();
		Map<ServiceKey, Set<ServiceKey>> instanceDependencies = new HashMap<>();
		IdentityHashMap<Object, CachedService> cache = new IdentityHashMap<>();

		ServiceGraph serviceGraph = ServiceGraph.create()
				.withNodeSuffixes(key -> {
					ServiceKey serviceKey = (ServiceKey) key;
					if (!serviceKey.isWorker()) {
						return null;
					}
					return pools.get(serviceKey.getWorkerPoolId() - 1).getSize();
				})
				.initialize(initializer);

		IdentityHashMap<Object, ServiceKey> workerInstanceToKey = new IdentityHashMap<>();
		if (workerPools != null) {
			for (int i = 0; i < pools.size(); i++) {
				WorkerPool pool = pools.get(i);
				int idx = i + 1;
				Map<Key<?>, Set<ScopedValue<Dependency>>> scopeDependencies = getScopeDependencies(injector, pool.getScope());
				for (Map.Entry<Key<?>, WorkerPool.Instances<?>> entry : pool.peekInstances().entrySet()) {
					Key<?> key = entry.getKey();
					WorkerPool.Instances<?> workerInstances = entry.getValue();
					if (!scopeDependencies.containsKey(key)) continue;
					ServiceKey serviceKey = new ServiceKey(key, idx);
					instances.put(serviceKey, workerInstances.getList());
					workerInstanceToKey.put(workerInstances.get(0), serviceKey);
					instanceDependencies.put(serviceKey,
							scopeDependencies.get(key)
									.stream()
									.filter(scopedDependency -> scopedDependency.get().isRequired() ||
											(scopedDependency.isScoped() ?
													pool.getScopeInjectors()[0].hasInstance(scopedDependency.get().getKey()) :
													injector.hasInstance(scopedDependency.get().getKey())))
									.map(scopedDependency -> scopedDependency.isScoped() ?
											new ServiceKey(scopedDependency.get().getKey(), idx) :
											new ServiceKey(scopedDependency.get().getKey()))
									.collect(toSet()));
				}
			}
		}

		for (Map.Entry<Key<?>, Object> entry : injector.peekInstances().entrySet()) {
			Key<?> key = entry.getKey();
			Object instance = entry.getValue();
			if (instance == null) continue;
			Binding<?> binding = injector.getBinding(key);
			if (binding == null) continue;
			ServiceKey serviceKey = new ServiceKey(key);
			instances.put(serviceKey, singletonList(instance));
			instanceDependencies.put(serviceKey,
					Arrays.stream(binding.getDependencies())
							.filter(dependency -> dependency.isRequired() ||
									injector.hasInstance(dependency.getKey()))
							.map(dependency -> {
								Class<?> dependencyRawType = dependency.getKey().getRawType();
								boolean rawTypeMatches = dependencyRawType == WorkerPool.class || dependencyRawType == WorkerPools.class;
								boolean instanceMatches = instance instanceof WorkerPool.Instances;

								if (rawTypeMatches && instanceMatches) {
									WorkerPool.Instances<?> workerInstances = (WorkerPool.Instances<?>) instance;
									return workerInstanceToKey.get(workerInstances.get(0));
								}

								if (rawTypeMatches && !(instance instanceof WorkerPool)) {
									logger.log(Level.WARNING, () -> "Unsupported service " + key + " at " + binding.getLocation() + " : worker instances is expected");
								}

								if (instanceMatches) {
									logger.log(Level.WARNING, () -> "Unsupported service " + key + " at " + binding.getLocation() + " : dependency to WorkerPool or WorkerPools is expected");
								}
								return new ServiceKey(dependency.getKey());
							})
							.collect(toSet()));
		}

		return populateServiceGraph(serviceGraph, instances, instanceDependencies, cache);
	}

	private Map<Key<?>, Set<ScopedValue<Dependency>>> getScopeDependencies(Injector injector, Scope scope) {
		Trie<Scope, Map<Key<?>, Binding<?>>> scopeBindings = injector.getBindings().getOrDefault(scope, emptyMap());
		return scopeBindings.get()
				.entrySet()
				.stream()
				.collect(toMap(Map.Entry::getKey,
						entry -> Arrays.stream(entry.getValue().getDependencies())
								.map(dependencyKey ->
										scopeBindings.get().containsKey(dependencyKey.getKey()) ?
												ScopedValue.of(scope, dependencyKey) :
												ScopedValue.of(dependencyKey))
								.collect(toSet())));
	}

	@NotNull
	private ServiceGraph populateServiceGraph(ServiceGraph serviceGraph, Map<ServiceKey, List<?>> instances, Map<ServiceKey, Set<ServiceKey>> instanceDependencies, IdentityHashMap<Object, CachedService> cache) {
		Set<Key<?>> unusedKeys = difference(keys.keySet(), instances.keySet().stream().map(ServiceKey::getKey).collect(toSet()));
		if (!unusedKeys.isEmpty()) {
			logger.log(Level.WARNING, "Unused services : {}", unusedKeys);
		}

		for (Map.Entry<ServiceKey, List<?>> entry : instances.entrySet()) {
			ServiceKey serviceKey = entry.getKey();
			Service service = getWorkersServiceOrNull(cache, serviceKey, entry.getValue());
			serviceGraph.add(serviceKey, service);
		}

		for (Map.Entry<ServiceKey, Set<ServiceKey>> entry : instanceDependencies.entrySet()) {
			ServiceKey serviceKey = entry.getKey();
			Key<?> key = serviceKey.getKey();
			Set<ServiceKey> dependencies = new HashSet<>(entry.getValue());

			if (!difference(removedDependencies.getOrDefault(key, emptySet()), dependencies).isEmpty()) {
				logger.log(Level.WARNING, "Unused removed dependencies for {} : {}",
						new Object[]{key, difference(removedDependencies.getOrDefault(key, emptySet()), dependencies)
				});
			}

			if (!intersection(dependencies, addedDependencies.getOrDefault(key, emptySet())).isEmpty()) {
				logger.log(Level.WARNING, "Unused added dependencies for {} : {}",
						new Object[]{key, intersection(dependencies, addedDependencies.getOrDefault(key, emptySet()))});
			}

			Set<Key<?>> added = addedDependencies.getOrDefault(key, emptySet());
			for (Key<?> k : added) {
				List<ServiceKey> found = instances.keySet().stream().filter(s -> s.getKey().equals(k)).collect(toList());
				if (found.isEmpty()) {
					throw new IllegalArgumentException();
				}
				if (found.size() > 1) {
					throw new IllegalArgumentException();
				}
				dependencies.add(found.get(0));
			}

			Set<Key<?>> removed = removedDependencies.getOrDefault(key, emptySet());
			dependencies.removeIf(k -> removed.contains(k.getKey()));

			for (ServiceKey dependency : dependencies) {
				serviceGraph.add(serviceKey, dependency);
			}
		}

		serviceGraph.removeIntermediateNodes();

		return serviceGraph;
	}

	@Nullable
	private Service getWorkersServiceOrNull(IdentityHashMap<Object, CachedService> cache, ServiceKey key, List<?> instances) {
		List<Service> services = new ArrayList<>();
		boolean found = false;
		for (Object instance : instances) {
			Service service = getServiceOrNull(cache, key, instance);
			services.add(service);
			if (service != null) {
				found = true;
			}
		}
		if (!found) {
			return null;
		}
		return new Service() {
			@Override
			public CompletableFuture<?> start() {
				List<CompletableFuture<?>> futures = new ArrayList<>();
				for (Service service : services) {
					futures.add(service != null ? service.start() : null);
				}
				return combineFutures(futures, Runnable::run);
			}

			@Override
			public CompletableFuture<?> stop() {
				List<CompletableFuture<?>> futures = new ArrayList<>();
				for (Service service : services) {
					futures.add(service != null ? service.stop() : null);
				}
				return combineFutures(futures, Runnable::run);
			}
		};
	}

	private static Throwable getRootCause(Throwable e) {
		Throwable cause;
		while ((cause = e.getCause()) != null) {
			e = cause;
		}
		return e;
	}

	private static CompletableFuture<?> combineFutures(List<CompletableFuture<?>> futures, Executor executor) {
		CompletableFuture<?> resultFuture = new CompletableFuture<>();
		AtomicInteger count = new AtomicInteger(futures.size());
		AtomicReference<Throwable> exception = new AtomicReference<>();
		for (CompletableFuture<?> future : futures) {
			CompletableFuture<?> finalFuture = future != null ? future : completedFuture(null);
			finalFuture.whenCompleteAsync((o, e) -> {
				if (e != null) {
					exception.set(getRootCause(e));
				}
				if (count.decrementAndGet() == 0) {
					if (exception.get() != null) {
						resultFuture.completeExceptionally(exception.get());
					} else {
						resultFuture.complete(null);
					}
				}
			}, executor);
		}
		return resultFuture;
	}

	@Nullable
	@SuppressWarnings("unchecked")
	private Service getServiceOrNull(IdentityHashMap<Object, CachedService> cache, ServiceKey key, Object instance) {
		checkNotNull(instance);
		CachedService service = cache.get(instance);
		if (service != null) {
			return service;
		}
		if (excludedKeys.contains(key.getKey())) {
			return null;
		}
		ServiceAdapter<?> serviceAdapter = keys.get(key.getKey());
		if (serviceAdapter == null) {
			List<Class<?>> foundRegisteredClasses = new ArrayList<>();
			l1:
			for (Map.Entry<Class<?>, ServiceAdapter<?>> entry : registeredServiceAdapters.entrySet()) {
				Class<?> registeredClass = entry.getKey();
				if (registeredClass.isAssignableFrom(instance.getClass())) {
					Iterator<Class<?>> iterator = foundRegisteredClasses.iterator();
					while (iterator.hasNext()) {
						Class<?> foundRegisteredClass = iterator.next();
						if (registeredClass.isAssignableFrom(foundRegisteredClass)) {
							continue l1;
						}
						if (foundRegisteredClass.isAssignableFrom(registeredClass)) {
							iterator.remove();
						}
					}
					foundRegisteredClasses.add(registeredClass);
				}
			}

			if (foundRegisteredClasses.size() == 1) {
				serviceAdapter = registeredServiceAdapters.get(foundRegisteredClasses.get(0));
			}
			if (foundRegisteredClasses.size() > 1) {
				throw new IllegalArgumentException("Ambiguous services found for " + instance.getClass() +
						" : " + foundRegisteredClasses + ". Use register() methods to specify service.");
			}
		}
		if (serviceAdapter != null) {
			ServiceAdapter<Object> finalServiceAdapter = (ServiceAdapter<Object>) serviceAdapter;
			Service asyncService = new Service() {
				@Override
				public CompletableFuture<?> start() {
					return finalServiceAdapter.start(instance, executor);
				}

				@Override
				public CompletableFuture<?> stop() {
					return finalServiceAdapter.stop(instance, executor);
				}
			};
			service = new CachedService(asyncService);
			cache.put(instance, service);
			return service;
		}
		return null;
	}

	private static class CachedService implements Service {
		private final Service service;
		private CompletableFuture<?> startFuture;
		private CompletableFuture<?> stopFuture;

		private CachedService(Service service) {
			this.service = service;
		}

		@Override
		synchronized public CompletableFuture<?> start() {
			checkState(stopFuture == null, "Already stopped");
			if (startFuture == null) {
				startFuture = service.start();
			}
			return startFuture;
		}

		@Override
		synchronized public CompletableFuture<?> stop() {
			checkState(startFuture != null, "Has not been started yet");
			if (stopFuture == null) {
				stopFuture = service.stop();
			}
			return stopFuture;
		}
	}

}
