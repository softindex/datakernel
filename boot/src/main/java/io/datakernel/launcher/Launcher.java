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

import com.google.inject.Guice;
import com.google.inject.Inject;
import com.google.inject.Injector;
import com.google.inject.Stage;
import io.datakernel.annotation.Nullable;
import io.datakernel.config.ConfigModule;
import io.datakernel.jmx.ConcurrentJmxMBean;
import io.datakernel.jmx.JmxAttribute;
import io.datakernel.service.ServiceGraph;
import io.datakernel.service.ServiceGraphModule;
import org.slf4j.Logger;

import java.time.Duration;
import java.time.Instant;
import java.util.Collection;
import java.util.concurrent.CountDownLatch;

import static com.google.inject.util.Modules.combine;
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
	protected final Logger logger = getLogger(this.getClass());

	protected String[] args = new String[]{};

	@Inject(optional = true)
	@Nullable
	protected ServiceGraph serviceGraph;

	private volatile Throwable applicationError;

	private volatile Instant instantOfStart;
	private volatile Instant instantOfRun;
	private volatile Instant instantOfStop;
	private volatile Instant instantOfComplete;

	private final CountDownLatch shutdownLatch = new CountDownLatch(1);

	private final Thread mainThread = Thread.currentThread();

	/**
	 * Supplies modules for application(ConfigModule, EventloopModule, etc...)
	 *
	 * @return
	 */
	protected abstract Collection<com.google.inject.Module> getModules();

	/**
	 * Creates a Guice injector with modules and overrides from this launcher and
	 * a special module which creates a members injector for this launcher.
	 * Both of those are unused on their own, but on creation they do all the binding checks
	 * so calling this method causes an exception to be thrown on any incorrect bindings
	 * which is highly for testing.
	 */
	public final void testInjector() {
		createInjector(Stage.TOOL, new String[0]);
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
	 * @param args                program args that will be injected into @Args string array
	 * @param eagerSingletonsMode passed to Guice
	 */
	public void launch(boolean eagerSingletonsMode, String[] args) throws Exception {
		instantOfStart = Instant.now();
		Injector injector = createInjector(eagerSingletonsMode ? Stage.PRODUCTION : Stage.DEVELOPMENT, args);
		logger.info("=== INJECTING DEPENDENCIES");
		try {
			onStart();
			try {
				doStart();
				logger.info("=== RUNNING APPLICATION");
				instantOfRun = Instant.now();
				run();
			} catch (Throwable e) {
				this.applicationError = e;
				throw e;
			} finally {
				instantOfStop = Instant.now();
				doStop();
			}
		} catch (Exception e) {
			if (this.applicationError == null) this.applicationError = e;
			logger.error("Application failure", e);
			throw e;
		} finally {
			onStop();
			instantOfComplete = Instant.now();
		}
	}

	synchronized public Injector createInjector(Stage stage, String[] args) {
		this.args = args;
		return Guice.createInjector(stage,
				combine(getModules()),
				binder -> binder.bind(String[].class).annotatedWith(Args.class).toInstance(args),
				binder -> binder.requestInjection(this),
				binder -> binder.bind(Launcher.class).toInstance(this));
	}

	private void doStart() throws Exception {
		if (serviceGraph != null) {
			logger.info("=== STARTING APPLICATION");
			try {
				serviceGraph.startFuture().get();
			} finally {
				logger.info("Services graph: \n" + serviceGraph);
			}
		}
	}

	/**
	 * This method runs when application is starting
	 *
	 * @throws Exception
	 */
	protected void onStart() throws Exception {
	}

	/**
	 * Launcher's main method.
	 *
	 * @throws Exception
	 */
	protected abstract void run() throws Exception;

	/**
	 * This method runs when application is stopping
	 *
	 * @throws Exception
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
				mainThread.join();
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

//	@JmxAttribute
//	@Nullable
//	public final Instant getInstantOfComplete() {
//		return instantOfComplete;
//	}

	@JmxAttribute
	@Nullable
	public final Duration getDurationOfStart() {
		if (instantOfStart == null) return null;
		return Duration.between(instantOfStart, instantOfRun == null ? Instant.now() : instantOfRun);
	}

	@JmxAttribute
	@Nullable
	public final Duration getDurationOfRun() {
		if (instantOfRun == null) return null;
		return Duration.between(instantOfRun, instantOfStop == null ? Instant.now() : instantOfStop);
	}

	@JmxAttribute
	@Nullable
	public final Duration getDurationOfStop() {
		if (instantOfStop == null) return null;
		return Duration.between(instantOfStop, instantOfComplete == null ? Instant.now() : instantOfComplete);
	}

	@JmxAttribute
	@Nullable
	public final Duration getDuration() {
		if (instantOfStart == null) return null;
		return Duration.between(instantOfStart, instantOfComplete == null ? Instant.now() : instantOfComplete);
	}

	@JmxAttribute
	@Nullable
	public final Throwable getApplicationError() {
		return applicationError;
	}
}
