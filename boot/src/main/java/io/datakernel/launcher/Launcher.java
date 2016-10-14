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
import io.datakernel.jmx.JmxRegistrator;
import io.datakernel.service.ServiceGraph;
import org.slf4j.Logger;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.slf4j.LoggerFactory.getLogger;

public abstract class Launcher {
	protected final Logger logger = getLogger(this.getClass());

	protected String[] args;

	private JmxRegistrator jmxRegistrator;

	private Stage stage;
	private Module[] modules;

	@Inject
	protected Provider<ServiceGraph> serviceGraphProvider;

	@Inject
	protected ShutdownNotification shutdownNotification;

	private final Thread mainThread = Thread.currentThread();

	public static <T extends Launcher> void main(Class<T> launcherClass, String[] args) throws Exception {
		T launcher = launcherClass.newInstance();
		launcher.launch(args);
	}

	public Launcher(Stage stage, Module... modules) {
		this.stage = stage;
		this.modules = modules;
	}

	public List<Module> getModules() {
		List<Module> moduleList = new ArrayList<>(Arrays.asList(this.modules));
		moduleList.add(new AbstractModule() {
			@Override
			protected void configure() {
				bind(String[].class).annotatedWith(Args.class).toInstance(args != null ? args : new String[]{});
			}
		});
		return moduleList;
	}

	public final Injector testInjector() {
		List<Module> modules = getModules();
		modules.add(new AbstractModule() {
			@Override
			protected void configure() {
				bind((Class<?>)Launcher.this.getClass());
			}
		});
		return Guice.createInjector(Stage.TOOL, modules);
	}

	public void launch(String[] args) throws Exception {
		this.args = args;
		Injector injector = Guice.createInjector(stage, getModules());
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

	private void doInject(Injector injector) throws Exception {
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
		addShutdownHook();
		shutdownNotification.await();
	}

	private void addShutdownHook() {
		Runtime.getRuntime().addShutdownHook(new Thread("shutdownNotification") {
			@Override
			public void run() {
				try {
					shutdownNotification.requestShutdown();
					mainThread.join();
				} catch (InterruptedException e) {
					logger.error("Failed shutdown", e);
				}
			}
		});
	}
}
