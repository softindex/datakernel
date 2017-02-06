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

import com.google.common.base.Throwables;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.SetMultimap;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import com.google.inject.*;
import com.google.inject.matcher.AbstractMatcher;
import com.google.inject.spi.*;
import io.datakernel.eventloop.Eventloop;
import io.datakernel.eventloop.EventloopServer;
import io.datakernel.eventloop.EventloopService;
import io.datakernel.net.BlockingSocketServer;
import io.datakernel.worker.WorkerPoolModule;
import io.datakernel.worker.WorkerPoolObjects;
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

public final class ServiceGraphModule extends AbstractModule {
	private final Logger logger = getLogger(this.getClass());

	private final Map<Class<?>, ServiceAdapter<?>> registeredServiceAdapters = new LinkedHashMap<>();
	private final Set<Key<?>> excludedKeys = new LinkedHashSet<>();
	private final Map<Key<?>, ServiceAdapter<?>> keys = new LinkedHashMap<>();

	private final SetMultimap<Key<?>, Key<?>> addedDependencies = HashMultimap.create();
	private final SetMultimap<Key<?>, Key<?>> removedDependencies = HashMultimap.create();

	private final Set<Key<?>> singletonKeys = new HashSet<>();
	private final Set<Key<?>> workerKeys = new HashSet<>();
	private final SetMultimap<Key<?>, Key<?>> workerDependencies = HashMultimap.create();

	private final IdentityHashMap<Object, CachedService> services = new IdentityHashMap<>();

	private final Executor executor;

	private WorkerPoolModule workerPoolModule;

	private ServiceGraph serviceGraph;

	private ServiceGraphModule() {
		this.executor = new ThreadPoolExecutor(0, Integer.MAX_VALUE,
				10, TimeUnit.MILLISECONDS,
				new SynchronousQueue<Runnable>());
	}

	public static ServiceGraphModule defaultInstance() {
		return newInstance()
				.register(Service.class, ServiceAdapters.forService())
				.register(BlockingService.class, ServiceAdapters.forBlockingService())
				.register(BlockingSocketServer.class, ServiceAdapters.forBlockingSocketServer())
				.register(Closeable.class, ServiceAdapters.forCloseable())
				.register(ExecutorService.class, ServiceAdapters.forExecutorService())
				.register(Timer.class, ServiceAdapters.forTimer())
				.register(DataSource.class, ServiceAdapters.forDataSource())
				.register(EventloopService.class, ServiceAdapters.forEventloopService())
				.register(EventloopServer.class, ServiceAdapters.forEventloopServer())
				.register(Eventloop.class, ServiceAdapters.forEventloop());
	}

	public static ServiceGraphModule newInstance() {
		return new ServiceGraphModule();
	}

	private static boolean isSingleton(Binding<?> binding) {
		return binding.acceptScopingVisitor(new BindingScopingVisitor<Boolean>() {
			@Override
			public Boolean visitNoScoping() {
				return false;
			}

			@Override
			public Boolean visitScopeAnnotation(Class<? extends Annotation> visitedAnnotation) {
				return visitedAnnotation.equals(Singleton.class);
			}

			@Override
			public Boolean visitScope(Scope visitedScope) {
				return visitedScope.equals(Scopes.SINGLETON);
			}

			@Override
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
				} catch (ReflectiveOperationException ignored) {
				}
			}
		}
		String simpleName = annotation.annotationType().getSimpleName();
		return "@" + ("NamedImpl".equals(simpleName) ? "Named" : simpleName) + (first ? "" : "(" + sb + ")");
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

	private Service getWorkersServiceOrNull(final Key<?> key, final List<?> instances) {
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
				return combineFutures(futures, directExecutor());
			}

			@Override
			public ListenableFuture<?> stop() {
				List<ListenableFuture<?>> futures = new ArrayList<>();
				for (Service service : services) {
					futures.add(service != null ? service.stop() : null);
				}
				return combineFutures(futures, directExecutor());
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
	private Service getServiceOrNull(Key<?> key, final Object instance) {
		checkNotNull(instance);
		CachedService service = services.get(instance);
		if (service != null) {
			return service;
		}
		if (excludedKeys.contains(key)) {
			return null;
		}
		ServiceAdapter<?> serviceAdapter = keys.get(key);
		if (serviceAdapter == null) {
			List<Class<?>> foundRegisteredClasses = new ArrayList<>();
			l1:
			for (Map.Entry<Class<?>, ServiceAdapter<?>> entry : registeredServiceAdapters.entrySet()) {
				Class<?> registeredClass = entry.getKey();
				if (registeredClass.isAssignableFrom(instance.getClass())) {
					Iterator<Class<?>> iterator = foundRegisteredClasses.iterator();
					while (iterator.hasNext()) {
						Class<?> foundRegisteredClass = iterator.next();
						if (registeredClass.isAssignableFrom(foundRegisteredClass))
							continue l1;
						if (foundRegisteredClass.isAssignableFrom(registeredClass))
							iterator.remove();
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
			final ServiceAdapter finalServiceAdapter = serviceAdapter;
			Service asyncService = new Service() {
				@Override
				public ListenableFuture<?> start() {
					return finalServiceAdapter.start(instance, executor);
				}

				@Override
				public ListenableFuture<?> stop() {
					return finalServiceAdapter.stop(instance, executor);
				}
			};
			service = new CachedService(asyncService);
			services.put(instance, service);
			return service;
		}
		return null;
	}

	private void createGuiceGraph(final Injector injector, final ServiceGraph graph) {
		if (!difference(keys.keySet(), injector.getAllBindings().keySet()).isEmpty()) {
			logger.warn("Unused services : {}", difference(keys.keySet(), injector.getAllBindings().keySet()));
		}

		for (Key<?> key : singletonKeys) {
			Object instance = injector.getInstance(key);
			Service service = getServiceOrNull(key, instance);
			graph.add(key, service);
		}

		for (Key<?> key : workerKeys) {
			WorkerPoolObjects poolObjects = workerPoolModule.getPoolObjects(key);
			Service service = getWorkersServiceOrNull(key, poolObjects.getObjects());
			graph.add(key, service);
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
		workerPoolModule = new WorkerPoolModule();
		install(workerPoolModule);
		bindListener(new AbstractMatcher<Binding<?>>() {
			@Override
			public boolean matches(Binding<?> binding) {
				return WorkerPoolModule.isWorkerScope(binding);
			}
		}, new ProvisionListener() {
			@Override
			public <T> void onProvision(ProvisionInvocation<T> provision) {
				synchronized (ServiceGraphModule.this) {
					if (serviceGraph != null) {
						logger.warn("Service graph already started, ignoring {}", provision.getBinding().getKey());
						return;
					}
					if (provision.provision() != null) {
						workerKeys.add(provision.getBinding().getKey());
					}
					List<DependencyAndSource> chain = provision.getDependencyChain();
					if (chain.size() >= 2) {
						Key<?> key = chain.get(chain.size() - 2).getDependency().getKey();
						Key<T> dependencyKey = provision.getBinding().getKey();
						if (key.getTypeLiteral().getRawType() != ServiceGraph.class) {
							workerDependencies.put(key, dependencyKey);
						}
					}
				}
			}
		});
		bindListener(new AbstractMatcher<Binding<?>>() {
			@Override
			public boolean matches(Binding<?> binding) {
				return isSingleton(binding);
			}
		}, new ProvisionListener() {
			@Override
			public <T> void onProvision(ProvisionInvocation<T> provision) {
				synchronized (ServiceGraphModule.this) {
					if (serviceGraph != null) {
						logger.warn("Service graph already started, ignoring {}", provision.getBinding().getKey());
						return;
					}
					if (provision.provision() != null) {
						singletonKeys.add(provision.getBinding().getKey());
					}
				}
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
	synchronized ServiceGraph serviceGraph(final Injector injector) {
		if (serviceGraph == null) {
			serviceGraph = new ServiceGraph() {
				@Override
				protected String nodeToString(Object node) {
					Key<?> key = (Key<?>) node;
					Annotation annotation = key.getAnnotation();
					WorkerPoolObjects poolObjects = workerPoolModule.getPoolObjects(key);
					return key.getTypeLiteral() +
							(annotation != null ? " " + prettyPrintAnnotation(annotation) : "") +
							(poolObjects != null ? " [" + poolObjects.getWorkerPool().getWorkersCount() + "]" : "");
				}
			};
			createGuiceGraph(injector, serviceGraph);
			serviceGraph.removeIntermediateNodes();
			logger.info("Services graph: \n" + serviceGraph);
		}
		return serviceGraph;
	}

	private class CachedService implements Service {
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

}