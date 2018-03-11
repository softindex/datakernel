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

import com.google.inject.*;
import io.datakernel.config.ConfigModule;
import io.datakernel.service.ServiceGraph;
import io.datakernel.service.ServiceGraphModule;
import org.slf4j.Logger;

import java.util.Collection;

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
 * <pre><code>
 * public class ApplicationLauncher extends Launcher {
 *
 *    public ApplicationLauncher() {
 *        super({@link Stage Stage.PRODUCTION},
 *            {@link ServiceGraphModule}.defaultInstance(),
 *            new DaoTierModule(),
 *            new ControllerTierModule(),
 *            new ViewTierModule(),
 *            {@link ConfigModule}.create(PropertiesConfig.ofProperties("props.properties"))
 *    }
 *
 *    public static void main(String[] args) throws Exception {
 *        ApplicationLauncher launcher = new ApplicationLauncher();
 *        launcher.launch(args);
 *    }
 * }
 * </code></pre>
 *
 * @see ServiceGraph
 * @see ServiceGraphModule
 * @see ConfigModule
 */
public abstract class Launcher {
	protected final Logger logger = getLogger(this.getClass());

	protected String[] args = new String[]{};

	@Inject
	protected ServiceGraph serviceGraph;

	@Inject
	protected ShutdownNotification shutdownNotification;

	private final Thread mainThread = Thread.currentThread();

	protected abstract Collection<Module> getModules();

	/**
	 * Creates a Guice injector with modules and overrides from this launcher and
	 * a special module which creates a members injector for this launcher.
	 * Both of those are unused on their own, but on creation they do all the binding checks
	 * so calling this method causes an exception to be thrown on any incorrect bindings
	 * which is highly for testing.
	 */
	public final void testInjector() {
		Guice.createInjector(Stage.TOOL,
				getCombinedModule(new String[0]),
				binder -> binder.getMembersInjector(Launcher.this.getClass()));
	}

	public void launch(boolean eagerSingletonsMode, String[] args) throws Exception {
		Injector injector = Guice.createInjector(eagerSingletonsMode ? Stage.PRODUCTION : Stage.DEVELOPMENT,
				getCombinedModule(args));
		logger.info("=== INJECTING DEPENDENCIES");
		try {
			onStart();
			try {
				logger.info("=== STARTING APPLICATION");
				doStart();
				logger.info("=== RUNNING APPLICATION");
				run();
			} finally {
				logger.info("=== STOPPING APPLICATION");
				doStop();
			}
		} catch (Exception e) {
			logger.error("Application failure", e);
			throw e;
		} finally {
			onStop();
		}
	}

	public Module getCombinedModule(String[] args) {
		this.args = args;
		return combine(
				combine(getModules()),
				binder -> binder.bind(String[].class).annotatedWith(Args.class).toInstance(args),
				binder -> binder.requestInjection(this));
	}

	private void doStart() throws Exception {
		serviceGraph.startFuture().get();
	}

	protected void onStart() throws Exception {
	}

	protected abstract void run() throws Exception;

	protected void onStop() throws Exception {
	}

	private void doStop() throws Exception {
		serviceGraph.stopFuture().get();
	}

	protected final void awaitShutdown() throws InterruptedException {
		Runtime.getRuntime().addShutdownHook(new Thread(() -> {
			try {
				shutdownNotification.requestShutdown();
				mainThread.join();
			} catch (InterruptedException e) {
				logger.error("Shutdown took too long", e);
			}
		}, "shutdownNotification"));
		shutdownNotification.await();
	}

	protected final void requestShutdown() {
		shutdownNotification.requestShutdown();
	}

}
