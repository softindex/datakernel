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

import io.datakernel.di.core.Injector;
import io.datakernel.di.core.Key;
import io.datakernel.di.module.AbstractModule;
import io.datakernel.di.module.Module;
import io.datakernel.jmx.ConcurrentJmxMBean;
import io.datakernel.jmx.JmxAttribute;
import org.jetbrains.annotations.Nullable;
import org.slf4j.Logger;

import java.time.Duration;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.*;

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
 */
public abstract class Launcher implements ConcurrentJmxMBean {
	protected final Logger logger = getLogger(getClass());
	private final Logger logger0 = getLogger(getClass().getName() + ".0");

	protected String[] args = {};

	private Thread mainThread;

	private volatile Throwable applicationError;

	private volatile Instant instantOfLaunch;
	private volatile Instant instantOfStart;
	private volatile Instant instantOfRun;
	private volatile Instant instantOfStop;
	private volatile Instant instantOfComplete;

	private final CountDownLatch shutdownLatch = new CountDownLatch(1);
	private final CountDownLatch completeLatch = new CountDownLatch(1);

	private final CompletableFuture<Void> onStartFuture = new CompletableFuture<>();
	private final CompletableFuture<Void> onRunFuture = new CompletableFuture<>();
	private final CompletableFuture<Void> onCompleteFuture = new CompletableFuture<>();

	/**
	 * Supplies modules for application(ConfigModule, EventloopModule, etc...)
	 */

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
	public void launch(String[] args) throws Exception {
		mainThread = Thread.currentThread();
		instantOfLaunch = Instant.now();

		try {
			logger.info("=== INJECTING DEPENDENCIES");

			Injector injector = createInjector(args);
			if (logger0.isInfoEnabled()) {
				logger0.info("Effective Injector:\n\n" + makeGraphVizGraph(injector.getBindings()));
			}

			logger0.info("EagerSingletons: " + injector.createEagerSingletons());

			Set<RootService> services = injector.getInstanceOr(new Key<Set<RootService>>() {}, emptySet());
			Set<RootService> startedServices = new HashSet<>();

			logger0.info("Post-inject instances: " + injector.postInjectInstances());

			logger.info("=== STARTING APPLICATION");
			try {
				instantOfStart = Instant.now();
				logger0.info("Starting RootServices: " + services);
				startServices(services, startedServices);
				onStart();
				onStartFuture.complete(null);
			} catch (Exception e) {
				applicationError = e;
				logger.error("Error", e);
				onStartFuture.completeExceptionally(e);
			}

			if (applicationError == null) {
				logger.info("=== RUNNING APPLICATION");
				try {
					instantOfRun = Instant.now();
					run();
					onRunFuture.complete(null);
				} catch (Exception e) {
					applicationError = e;
					logger.error("Error", e);
					onRunFuture.completeExceptionally(e);
				}
			} else {
				onRunFuture.completeExceptionally(applicationError);
			}

			logger.info("=== STOPPING APPLICATION");
			instantOfStop = Instant.now();
			if (!onStartFuture.isCompletedExceptionally()) {
				try {
					onStop();
				} catch (Exception e) {
					logger.error("Stop error", e);
				}
			}

			stopServices(startedServices);

			if (applicationError == null) {
				onCompleteFuture.complete(null);
			} else {
				onCompleteFuture.completeExceptionally(applicationError);
				throw applicationError;
			}

		} catch (Exception e) {
			throw e;
		} catch (Throwable e) {
			applicationError = e;
			logger.error("JVM Fatal Error", e);
			System.exit(-1);
		} finally {
			instantOfComplete = Instant.now();
			completeLatch.countDown();
		}
	}

	private void startServices(Collection<RootService> services, Collection<RootService> startedServices) throws Throwable {
		List<Throwable> exceptions = new ArrayList<>();
		CountDownLatch latch = new CountDownLatch(services.size());
		synchronized (this) {
			for (RootService service : services) {
				if (!exceptions.isEmpty()) {
					latch.countDown();
					continue;
				}
				logger0.info("Starting RootService: " + service);
				service.start().whenComplete(($, e) -> {
					synchronized (this) {
						if (e == null) {
							startedServices.add(service);
						} else {
							exceptions.add(
									(e instanceof CompletionException || e instanceof ExecutionException) && e.getCause() != null ? e.getCause() : e);
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
	}

	private void stopServices(Collection<RootService> startedServices) throws InterruptedException {
		CountDownLatch latch = new CountDownLatch(startedServices.size());
		for (RootService service : startedServices) {
			logger0.info("Stopping RootService: " + service);
			service.stop().whenComplete(($, e) -> {
				if (e != null) {
					logger.error("Stop error in " + service,
							(e instanceof CompletionException || e instanceof ExecutionException) && e.getCause() != null ? e.getCause() : e);
				}
				latch.countDown();
			});
		}
		latch.await();
	}

	public Injector createInjector(String[] args) {
		this.args = args;
		return createInjector();
	}

	private Injector createInjector() {
		return Injector.of(getLauncherModule().combineWith(getModule()).overrideWith(getOverrideModule()));
	}

	private Module getLauncherModule() {
		return new AbstractModule() {
			@Override
			protected void configure() {
				bind(String[].class).annotatedWith(Args.class).toInstance(args);

				//noinspection unchecked
				Class<Launcher> launcherClass = (Class<Launcher>) Launcher.this.getClass();
				bind(Launcher.class).to(launcherClass);
				bind(launcherClass).toInstance(Launcher.this);

				postInjectInto(launcherClass);

				Key<CompletionStage<Void>> completionStage = new Key<CompletionStage<Void>>() {};
				bind(completionStage.named(OnStart.class)).toInstance(onStartFuture);
				bind(completionStage.named(OnRun.class)).toInstance(onRunFuture);
				bind(completionStage.named(OnComplete.class)).toInstance(onCompleteFuture);

				addDeclarativeBindingsFrom(Launcher.this);
			}
		};
	}

	protected Module getModule() {
		return Module.empty();
	}

	protected Module getOverrideModule() {
		return Module.empty();
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

	public Thread getMainThread() {
		return mainThread;
	}

	public String[] getArgs() {
		return args;
	}

	public final CompletionStage<Void> getStartFuture() {
		return onStartFuture;
	}

	public final CompletionStage<Void> getRunFuture() {
		return onRunFuture;
	}

	public final CompletionStage<Void> getCompleteFuture() {
		return onCompleteFuture;
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
