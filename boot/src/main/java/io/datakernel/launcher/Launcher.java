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
import com.google.inject.util.Modules;
import io.datakernel.config.ConfigsModule;
import io.datakernel.jmx.JmxRegistrator;
import io.datakernel.service.ServiceGraph;
import io.datakernel.service.ServiceGraphModule;
import org.slf4j.Logger;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import static java.util.Arrays.asList;
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
 *            {@link ConfigsModule}.create(PropertiesConfig.ofProperties("props.properties"))
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
 * @see ConfigsModule
 */
public abstract class Launcher {
	protected final Logger logger = getLogger(this.getClass());

	private JmxRegistrator jmxRegistrator;

	private Stage stage;
	private Module[] baseModules;
	private List<Module> additionalModules = new ArrayList<>();
	private List<Module> overrides = new ArrayList<>();

	@Inject
	protected Provider<ServiceGraph> serviceGraphProvider;

	@Inject
	protected ShutdownNotification shutdownNotification;

	private final Thread mainThread = Thread.currentThread();

	public Launcher(Stage stage, Module... baseModules) {
		this.stage = stage;
		this.baseModules = baseModules;
	}

	// region addModules
	public Launcher addModule(Module module) {
		additionalModules.add(module);
		return this;
	}

	public Launcher addModules(Module... modules) {
		return addModules(asList(modules));
	}

	public Launcher addModules(Collection<Module> modules) {
		this.additionalModules.addAll(modules);
		return this;
	}
	// endregion

	// region addOverrides
	public Launcher addOverride(Module override) {
		overrides.add(override);
		return this;
	}

	public Launcher addOverrides(Module... overrides) {
		return addOverrides(asList(overrides));
	}

	public Launcher addOverrides(Collection<Module> overrides) {
		this.overrides.addAll(overrides);
		return this;
	}
	// endregion

	/**
	 * Creates a Guice injector with modules and overrides from this launcher and
	 * a special module which creates a members injector for this launcher.
	 * Both of those are unused on their own, but on creation they do all the binding checks
	 * so calling this method causes an exception to be thrown on any incorrect bindings
	 * which is highly for testing.
	 */
	public final void testInjector() {
		Guice.createInjector(Stage.TOOL, createFinalModule(new String[0]), binder -> binder.getMembersInjector(Launcher.this.getClass()));
	}

	public void launch(String[] args) throws Exception {
		Injector injector = Guice.createInjector(stage, createFinalModule(args));
		logger.info("=== INJECTING DEPENDENCIES");
		doInject(injector);
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

	private Module createFinalModule(String[] args) {
		List<Module> modules = new ArrayList<>();
		modules.add(Modules.override(baseModules).with(overrides));
		modules.add(binder -> binder.bind(String[].class).annotatedWith(Args.class).toInstance(args));
		modules.addAll(additionalModules);
		return Modules.combine(modules);
	}

	private void doInject(Injector injector) {
		injector.injectMembers(this);
		Binding<JmxRegistrator> binding = injector.getExistingBinding(Key.get(JmxRegistrator.class));
		if (binding != null) {
			jmxRegistrator = binding.getProvider().get();
		}
	}

	private void doStart() throws Exception {
		if (jmxRegistrator != null) {
			jmxRegistrator.registerJmxMBeans();
		} else {
			logger.info("Jmx is disabled. Add JmxModule to enable.");
		}
		serviceGraphProvider.get().startFuture().get();
	}

	protected void onStart() throws Exception {
	}

	protected abstract void run() throws Exception;

	protected void onStop() throws Exception {
	}

	private void doStop() throws Exception {
		serviceGraphProvider.get().stopFuture().get();
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
