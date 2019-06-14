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

package io.datakernel.launcher;

import io.datakernel.config.ConfigModule;
import io.datakernel.di.core.Injector;
import io.datakernel.di.core.InstanceInjector;
import io.datakernel.di.core.Key;
import io.datakernel.di.module.AbstractModule;
import io.datakernel.di.module.Module;
import io.datakernel.di.module.Multibinder;
import io.datakernel.di.util.Types;
import io.datakernel.jmx.ConcurrentJmxMBean;
import io.datakernel.jmx.JmxAttribute;
import io.datakernel.service.Service;
import io.datakernel.service.ServiceGraph;
import io.datakernel.service.ServiceGraphModule;
import org.jetbrains.annotations.Nullable;
import org.slf4j.Logger;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.CountDownLatch;

import static io.datakernel.di.module.Modules.combine;
import static io.datakernel.di.module.Modules.override;
import static io.datakernel.di.util.Utils.makeGraphVizGraph;
import static java.util.Collections.emptySet;
import static java.util.Comparator.comparingInt;
import static org.slf4j.LoggerFactory.getLogger;

/**
 * Integrates all modules together and manages application lifecycle by
 * passing several steps:
 * <ul>
 * <li>wiring modules</li>
 * <li>starting services</li>
 * <li>running</li>
 * <li>stopping services</li>
 * </ul>
 * <p>
 * Example.<br>
 * Prerequisites: an application consists of three modules, which preferably
 * should be configured using separate configs and may depend on each other.
 * <pre>
 * public class ApplicationLauncher extends Launcher {
 *
 *    &#64;Override
 *    protected Collection&#60;Module&#62; getModules() {
 *        return null;
 *    }
 *
 *    &#64;Override
 *    protected void run() {
 *        System.out.println("Hello world");
 *    }
 *
 *    public static void main(String[] args) throws Exception {
 *        ApplicationLauncher launcher = new ApplicationLauncher();
 *        launcher.launch(true, args);
 *    }
 * }
 * </pre>
 *
 * @see ServiceGraph
 * @see ServiceGraphModule
 * @see ConfigModule
 */
public abstract class Launcher implements ConcurrentJmxMBean {
	protected final Logger logger = getLogger(getClass());

	protected String[] args = {};

	private volatile Throwable applicationError;

	private volatile Instant instantOfLaunch;
	private volatile Instant instantOfStart;
	private volatile Instant instantOfRun;
	private volatile Instant instantOfStop;
	private volatile Instant instantOfComplete;

	private final CountDownLatch shutdownLatch = new CountDownLatch(1);
	private final CountDownLatch completeLatch = new CountDownLatch(1);

	private final CompletableFuture<Void> onStart = new CompletableFuture<>();
	private final CompletableFuture<Void> onRun = new CompletableFuture<>();
	private final CompletableFuture<Void> onComplete = new CompletableFuture<>();

	/**
	 * Supplies modules for application(ConfigModule, EventloopModule, etc...)
	 */
	protected Module getModule() {
		return Module.empty();
	}

	protected Module getOverrideModule() {
		return Module.empty();
	}

	/**
	 * Creates an injector with modules and overrides from this launcher.
	 * On creation it does all the binding checks so calling this method
	 * triggers a static check which causes an exception to be thrown on
	 * any incorrect bindings (unsatisfied or cyclic dependencies)
	 * which is highly useful for testing.
	 */
	public final void testInjector() {
		createInjector(new String[0]);
	}

	/**
	 * Launch application following few simple steps:
	 * <ul>
	 * <li>Inject dependencies</li>
	 * <li>Starts application, {@link Launcher#onStart()} is called in this stage</li>
	 * <li>Runs application, {@link Launcher#run()} is called in this stage</li>
	 * <li>Stops application, {@link Launcher#onStop()} is called in this stage</li>
	 * </ul>
	 * You can override methods mentioned above to execute your code in needed stage.
	 *
	 * @param args program args that will be injected into @Args string array
	 */
	@SuppressWarnings("unchecked")
	public void launch(String[] args) throws Exception {
		instantOfLaunch = Instant.now();

		try {
			logger.info("=== INJECTING DEPENDENCIES");

			Injector injector = createInjector(args);
			logger.trace("Dependency graph:\n" + makeGraphVizGraph(injector.getBindings()));

			injector.getInstanceOr(new Key<Set<Key<?>>>(EagerSingleton.class) {}, emptySet()).forEach(injector::getInstanceOrNull);

			for (InstanceInjector<?> instanceInjector : injector.getInstance(new Key<Set<InstanceInjector<?>>>() {})) {
				Object instance = injector.getInstanceOrNull(instanceInjector.key());
				if (instance != null) {
					((InstanceInjector<Object>) instanceInjector).inject(instance);
				}
			}

			Set<Service> services = injector.getInstanceOr(new Key<Set<Service>>() {}, emptySet());
			List<Service> startedServices = new ArrayList<>();

			logger.info("=== STARTING APPLICATION");
			try {
				instantOfStart = Instant.now();
				startedServices.addAll(startServices(services));
				onStart();
				onStart.complete(null);
			} catch (InterruptedException | RuntimeException e) {
				throw e;
			} catch (Exception e) {
				applicationError = e;
				logger.error("Error", e);
				onStart.completeExceptionally(e);
			}

			if (applicationError == null) {
				logger.info("=== RUNNING APPLICATION");
				try {
					instantOfRun = Instant.now();
					run();
					onRun.complete(null);
				} catch (InterruptedException | RuntimeException e) {
					throw e;
				} catch (Exception e) {
					applicationError = e;
					logger.error("Error", e);
					onRun.completeExceptionally(e);
					throw e;
				}
			} else {
				onRun.completeExceptionally(applicationError);
			}

			logger.info("=== STOPPING APPLICATION");
			instantOfStop = Instant.now();
			if (!onStart.isCompletedExceptionally()) {
				try {
					onStop();
				} catch (InterruptedException | RuntimeException e) {
					throw e;
				} catch (Exception e) {
					logger.error("Stop error", e);
				}
			}

			stopServices(startedServices);

			if (applicationError == null) {
				onComplete.complete(null);
			} else {
				onComplete.completeExceptionally(applicationError);
				throw applicationError;
			}

		} catch (InterruptedException | RuntimeException e) {
			applicationError = e;
			logger.error("Runtime Error", e);
			onStart.completeExceptionally(e);
			onRun.completeExceptionally(e);
			onComplete.completeExceptionally(e);
			throw e;
		} catch (Error e) {
			applicationError = e;
			logger.error("JVM Fatal Error", e);
			throw e;
		} catch (Throwable e) {
			applicationError = e;
			logger.error("JVM Fatal Error", e);
			throw new Exception(e);
		} finally {
			instantOfComplete = Instant.now();
			completeLatch.countDown();
		}
	}

	private List<Service> startServices(Collection<Service> services) throws Throwable {
		List<Service> startedServices = new ArrayList<>();
		List<Throwable> exceptions = new ArrayList<>();
		CountDownLatch latch = new CountDownLatch(services.size());
		synchronized (this) {
			for (Service service : services) {
				if (!exceptions.isEmpty()) {
					latch.countDown();
					continue;
				}
				service.start().whenComplete(($, e) -> {
					synchronized (this) {
						if (e == null) {
							startedServices.add(service);
						} else {
							exceptions.add(e);
						}
						latch.countDown();
					}
				});
			}
		}
		latch.await();
		if (!exceptions.isEmpty()) {
			exceptions.sort(comparingInt(e -> (e instanceof RuntimeException) ? 1 : (e instanceof Error ? 0 : 2)));
			exceptions.stream().skip(1).forEach(e -> exceptions.get(0).addSuppressed(e));
			throw exceptions.get(0);
		}
		return startedServices;
	}

	private void stopServices(List<Service> startedServices) throws InterruptedException {
		CountDownLatch latch = new CountDownLatch(startedServices.size());
		for (Service service : startedServices) {
			service.stop().whenComplete(($, e) -> {
				if (e != null) {
					logger.error("Stop error in " + service, e);
				}
				latch.countDown();
			});
		}
		latch.await();
	}

	@SuppressWarnings("unchecked")
	synchronized public Injector createInjector(String[] args) {
		this.args = args;
		return Injector.of(override(
				combine(
						getModule(),
						new AbstractModule() {{
							Class<Launcher> subclass = (Class<Launcher>) Launcher.this.getClass();

							bind(String[].class).annotatedWith(Args.class).toInstance(args);
							bind(Launcher.class).to(subclass);
							bind(subclass).toInstance(Launcher.this);

							bindIntoSet(new Key<InstanceInjector<?>>() {}, new Key<InstanceInjector<Launcher>>() {});
							bind(new Key<InstanceInjector<Launcher>>() {}).to(Key.ofType(Types.parameterized(InstanceInjector.class, subclass)));

							Key<CompletionStage<Void>> completionStageKey = new Key<CompletionStage<Void>>() {};

							bind(completionStageKey.named(OnStart.class)).toInstance(onStart);
							bind(completionStageKey.named(OnRun.class)).toInstance(onRun);
							bind(completionStageKey.named(OnComplete.class)).toInstance(onComplete);

							multibind(new Key<Set<Service>>() {}, Multibinder.toSet());

							addDeclarativeBindingsFrom(Launcher.this);
						}}),
				getOverrideModule())
		);
	}

	/**
	 * This method runs when application is starting
	 */
	protected void onStart() throws Exception {
	}

	/**
	 * Launcher's main method.
	 */
	protected abstract void run() throws Exception;

	/**
	 * This method runs when application is stopping
	 */
	protected void onStop() throws Exception {
	}

	/**
	 * Blocks current thread until shutdown notification releases it.
	 * <br>
	 * Shutdown notification is released on JVM shutdown or by calling {@link Launcher#shutdown()}
	 */
	protected final void awaitShutdown() throws InterruptedException {
		Runtime.getRuntime().addShutdownHook(new Thread(() -> {
			try {
				shutdown();
				completeLatch.await();
				Thread.sleep(10); // wait a bit for things outside `launch` call, such as JUnit finishing or whatever
			} catch (InterruptedException e) {
				logger.error("Shutdown took too long", e);
			}
		}, "shutdownNotification"));
		shutdownLatch.await();
	}

	/**
	 * Releases all threads waiting for shutdown.
	 *
	 * @see Launcher#awaitShutdown()
	 */
	public final void shutdown() {
		shutdownLatch.countDown();
	}

	public final CompletionStage<Void> getStartFuture() {
		return onStart;
	}

	public final CompletionStage<Void> getRunFuture() {
		return onRun;
	}

	public final CompletionStage<Void> getCompleteFuture() {
		return onComplete;
	}

	@JmxAttribute
	@Nullable
	public final Instant getInstantOfLaunch() {
		return instantOfLaunch;
	}

	@JmxAttribute
	@Nullable
	public final Instant getInstantOfStart() {
		return instantOfStart;
	}

	@JmxAttribute
	@Nullable
	public final Instant getInstantOfRun() {
		return instantOfRun;
	}

	@JmxAttribute
	@Nullable
	public final Instant getInstantOfStop() {
		return instantOfStop;
	}

	@JmxAttribute
	@Nullable
	public final Instant getInstantOfComplete() {
		return instantOfComplete;
	}

	@JmxAttribute
	@Nullable
	public final Duration getDurationOfStart() {
		if (instantOfLaunch == null) {
			return null;
		}
		return Duration.between(instantOfLaunch, instantOfRun == null ? Instant.now() : instantOfRun);
	}

	@JmxAttribute
	@Nullable
	public final Duration getDurationOfRun() {
		if (instantOfRun == null) {
			return null;
		}
		return Duration.between(instantOfRun, instantOfStop == null ? Instant.now() : instantOfStop);
	}

	@JmxAttribute
	@Nullable
	public final Duration getDurationOfStop() {
		if (instantOfStop == null) {
			return null;
		}
		return Duration.between(instantOfStop, instantOfComplete == null ? Instant.now() : instantOfComplete);
	}

	@JmxAttribute
	@Nullable
	public final Duration getDuration() {
		if (instantOfLaunch == null) {
			return null;
		}
		return Duration.between(instantOfLaunch, instantOfComplete == null ? Instant.now() : instantOfComplete);
	}

	@JmxAttribute
	@Nullable
	public final Throwable getApplicationError() {
		return applicationError;
	}
}
