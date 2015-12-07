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
import io.datakernel.config.Config;
import io.datakernel.guice.ConfigModule;
import io.datakernel.guice.ShutdownNotification;
import io.datakernel.service.ServiceGraph;
import io.datakernel.util.FileLocker;
import org.slf4j.Logger;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.slf4j.LoggerFactory.getLogger;

public abstract class Launcher {
	protected final Logger logger = getLogger(this.getClass());

	protected String[] args;

	private Module[] modules;

	private String[] configs;

	private FileLocker fileLocker;

	private boolean useLockFile;

	private boolean useProductionMode;

	@Inject
	protected Injector injector;

	@Inject
	protected ServiceGraph serviceGraph;

	@Inject
	protected ShutdownNotification shutdownNotification;

	private final Thread mainThread = Thread.currentThread();

	protected Config config;

	protected void useLockFile() {
		this.useLockFile = true;
	}

	protected void useProductionMode() {
		this.useProductionMode = true;
	}

	protected void configs(String... config) {
		this.configs = config;
	}

	protected void modules(Module... modules) {
		this.modules = modules;
	}

	protected abstract void configure();

	public static <T extends Launcher> void run(Class<T> mainClass, String[] args) throws Exception {
		T app = mainClass.newInstance();
		app.args = args;
		app.configure();
		app.run();
	}

	protected final void run() throws Exception {
		try {
			obtainLockFile();
			logger.info("=== WIRING APPLICATION");
			doWire();
			writeConfig();
			try {
				logger.info("=== STARTING APPLICATION");
				doStart();
				logger.info("=== RUNNING APPLICATION");
				doRun();
			} finally {
				logger.info("=== STOPPING APPLICATION");
				doStop();
				releaseLockFile();
			}
		} catch (Exception e) {
			logger.error("Application failed", e);
		}
	}

	private void obtainLockFile() {
		if (!useLockFile) return;
		fileLocker = FileLocker.obtainLockOrDie(getLockFileName());
	}

	private String getLockFileName() {
		return "." + this.getClass().getSimpleName().toLowerCase() + ".lock";
	}

	private void releaseLockFile() {
		if (fileLocker == null) return;
		fileLocker.releaseLock();
	}

	private void writeConfig() throws IOException {
		if (configs == null || configs[0] == null)
			return;
		int pos = configs[0].lastIndexOf('.');
		if (pos == -1) {
			config.saveToPropertiesFile(configs[0] + "-all");
			return;
		}
		config.saveToPropertiesFile(configs[0].substring(0, pos) + "-all" + configs[0].substring(pos));
	}

	protected void doWire() throws Exception {
		List<Module> modules = new ArrayList<>(Arrays.asList(this.modules));
		if (configs != null && configs.length != 0) {
			List<Config> configsList = new ArrayList<>();
			for (String config : configs) {
				configsList.add(Config.ofProperties(config));
			}
			config = Config.union(configsList);
			modules.add(new ConfigModule(config));
		}
		Stage stage = useProductionMode ? Stage.PRODUCTION : Stage.DEVELOPMENT;
		Injector injector = Guice.createInjector(stage, modules);
		injector.injectMembers(this);
	}

	protected void doStart() throws Exception {
		serviceGraph.start();
	}

	abstract protected void doRun() throws Exception;

	protected void doStop() throws Exception {
		serviceGraph.stop();
	}

	protected final void awaitShutdown() throws InterruptedException {
		addShutdownHook();
		shutdownNotification.await();
	}

	protected final void addShutdownHook() {
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
