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
import io.datakernel.config.ConfigModule.ConfigModuleService;
import io.datakernel.di.core.Injector;
import io.datakernel.di.core.InstanceInjector;
import io.datakernel.di.core.Key;
import io.datakernel.di.module.AbstractModule;
import io.datakernel.di.module.Module;
import io.datakernel.di.util.Types;
import io.datakernel.jmx.ConcurrentJmxMBean;
import io.datakernel.jmx.JmxAttribute;
import io.datakernel.jmx.JmxModule.JmxModuleService;
import io.datakernel.service.ServiceGraph;
import io.datakernel.service.ServiceGraphModule;
import io.datakernel.trigger.TriggersModule.TriggersModuleService;
import org.jetbrains.annotations.Nullable;
import org.slf4j.Logger;

import java.time.Duration;
import java.time.Instant;
import java.util.Collections;
import java.util.Set;
import java.util.concurrent.CountDownLatch;

import static io.datakernel.di.module.Modules.*;
import static java.util.Collections.emptySet;
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

	@Nullable
	protected ServiceGraph serviceGraph;

	private volatile Throwable applicationError;

	private volatile Instant instantOfStart;
	private volatile Instant instantOfRun;
	private volatile Instant instantOfStop;
	private volatile Instant instantOfComplete;

	private final CountDownLatch shutdownLatch = new CountDownLatch(1);
	private final CountDownLatch finishLatch = new CountDownLatch(1);

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
	 * Creates a Guice injector with modules and overrides from this launcher and
	 * a special module which creates a members injector for this launcher.
	 * Both of those are unused on their own, but on creation they do all the binding checks
	 * so calling this method causes an exception to be thrown on any incorrect bindings
	 * which is highly for testing.
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
		instantOfStart = Instant.now();
		logger.info("=== INJECTING DEPENDENCIES");
		Injector injector = createInjector(args);
		injector.getInstanceOr(new Key<Set<Key<?>>>(EagerSingleton.class) {}, emptySet()).forEach(injector::getInstanceOrNull);
		for (InstanceInjector<?> instanceInjector : injector.getInstance(new Key<Set<InstanceInjector<?>>>() {})) {
			Object instance = injector.getInstanceOrNull(instanceInjector.key());
			if (instance != null) ((InstanceInjector<Object>) instanceInjector).inject(instance);
		}
		try {
			injector.getInstanceOr(new Key<Set<Runnable>>(OnStart.class) {}, emptySet()).forEach(Runnable::run);
			onStart();
			try {
				doStart(injector);
				logger.info("=== RUNNING APPLICATION");
				instantOfRun = Instant.now();
				run();
			} catch (Throwable e) {
				applicationError = e;
				throw e;
			} finally {
				instantOfStop = Instant.now();
				doStop();
			}
		} catch (Exception e) {
			if (applicationError == null) {
				applicationError = e;
			}
			logger.error("Application failure", e);
			throw e;
		} finally {
			injector.getInstanceOr(new Key<Set<Runnable>>(OnStop.class) {}, emptySet()).forEach(Runnable::run);
			onStop();
			instantOfComplete = Instant.now();
			finishLatch.countDown();
		}
	}

	synchronized public Injector createInjector(String[] args) {
		this.args = args;
		return Injector.of(override(
				combine(
						getModule(),
						new AbstractModule() {{
							bind(String[].class).annotatedWith(Args.class).toInstance(args);
							bind(Launcher.class).to(Key.ofType(Launcher.this.getClass()));
							bind(Key.ofType(Launcher.this.getClass())).toInstance(Launcher.this);
							bind(new Key<InstanceInjector<Launcher>>() {})
									.to(Key.ofType(Types.parameterized(InstanceInjector.class, Launcher.this.getClass())));

							multibind(new Key<Set<Runnable>>(OnStart.class) {}, multibinderToSet());
							multibind(new Key<Set<Runnable>>(OnStop.class) {}, multibinderToSet());
							multibind(new Key<Set<InstanceInjector<?>>>() {}, multibinderToSet());

							bind(new Key<Set<InstanceInjector<?>>>() {}).to(Collections::singleton, new Key<InstanceInjector<Launcher>>() {});

							addDeclarativeBindingsFrom(Launcher.this);
						}}),
				getOverrideModule())
		);
	}

	private void doStart(Injector injector) throws Exception {
		injector.getInstanceOrNull(ConfigModuleService.class);
		injector.getInstanceOrNull(JmxModuleService.class);
		injector.getInstanceOrNull(TriggersModuleService.class);
		serviceGraph = injector.getInstanceOrNull(ServiceGraph.class);
		if (serviceGraph == null) {
			return;
		}
		logger.info("=== STARTING APPLICATION");
		try {
			serviceGraph.startFuture().get();
		} finally {
			logger.info("Services graph: \n" + serviceGraph);
		}
	}

	/**
	 * This method runs when application is starting
	 */
	@SuppressWarnings("RedundantThrows")
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

	private void doStop() throws Exception {
		if (serviceGraph != null) {
			logger.info("=== STOPPING APPLICATION");
			serviceGraph.stopFuture().get();
		}
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
				finishLatch.await();
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
	public final Duration getDurationOfStart() {
		if (instantOfStart == null) {
			return null;
		}
		return Duration.between(instantOfStart, instantOfRun == null ? Instant.now() : instantOfRun);
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
		if (instantOfStart == null) {
			return null;
		}
		return Duration.between(instantOfStart, instantOfComplete == null ? Instant.now() : instantOfComplete);
	}

	@JmxAttribute
	@Nullable
	public final Throwable getApplicationError() {
		return applicationError;
	}
}
