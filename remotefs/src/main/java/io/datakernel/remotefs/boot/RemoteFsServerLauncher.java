package io.datakernel.remotefs.boot;

import com.google.inject.Module;
import com.google.inject.Provides;
import com.google.inject.Singleton;
import io.datakernel.config.Config;
import io.datakernel.config.ConfigsModule;
import io.datakernel.config.impl.PropertiesConfig;
import io.datakernel.eventloop.Eventloop;
import io.datakernel.jmx.JmxModule;
import io.datakernel.launcher.Launcher;
import io.datakernel.launcher.modules.EventloopModule;
import io.datakernel.launcher.modules.ExecutorServiceModule;
import io.datakernel.remotefs.RemoteFsServer;
import io.datakernel.service.ServiceGraphModule;
import io.datakernel.util.guice.SimpleModule;

import java.util.Collection;
import java.util.concurrent.ExecutorService;

import static com.google.inject.util.Modules.override;
import static io.datakernel.config.ConfigConverters.ofPath;
import static io.datakernel.remotefs.boot.ConfigUtils.initializeRemoteFsServer;
import static java.lang.Boolean.parseBoolean;
import static java.util.Arrays.asList;
import static java.util.Collections.emptyList;
import static java.util.Collections.singleton;

public abstract class RemoteFsServerLauncher extends Launcher {
	public static final String PRODUCTION_MODE = "production";
	public static final String PROPERTIES_FILE = "remotefs-server.properties";
	public static final String PROPERTIES_FILE_EFFECTIVE = "remotefs-server.effective.properties";

	@Override
	protected final Collection<Module> getModules() {
		return singleton(override(getBaseModules()).with(getOverrideModules()));
	}

	protected final Collection<Module> getBaseModules() {
		return asList(
				ServiceGraphModule.defaultInstance(),
				JmxModule.create(),
				ConfigsModule.create(PropertiesConfig.ofProperties(PROPERTIES_FILE, true))
						.saveEffectiveConfigTo(PROPERTIES_FILE_EFFECTIVE),
				EventloopModule.create(),
				ExecutorServiceModule.create(),
				new SimpleModule() {
					@Provides
					@Singleton
					RemoteFsServer provideServer(Eventloop eventloop, ExecutorService executor, Config config) {
						return RemoteFsServer.create(eventloop, executor, config.get(ofPath(), "remotefs.server.storage"))
								.initialize(server -> initializeRemoteFsServer(server, config.getChild("remotefs.server")));
					}
				}
		);
	}

	protected Collection<Module> getOverrideModules() {
		return emptyList();
	}

	@Override
	protected void run() throws Exception {
		awaitShutdown();
	}

	public static void main(String[] args) throws Exception {
		Launcher launcher = new RemoteFsServerLauncher() {};
		launcher.launch(parseBoolean(System.getProperty(PRODUCTION_MODE)), args);
	}
}
