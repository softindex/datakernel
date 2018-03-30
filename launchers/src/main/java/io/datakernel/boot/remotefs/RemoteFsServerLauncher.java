package io.datakernel.boot.remotefs;

import com.google.inject.Inject;
import com.google.inject.Module;
import com.google.inject.Provides;
import com.google.inject.Singleton;
import io.datakernel.config.Config;
import io.datakernel.config.ConfigConverters;
import io.datakernel.config.ConfigModule;
import io.datakernel.eventloop.Eventloop;
import io.datakernel.eventloop.ThrottlingController;
import io.datakernel.jmx.JmxModule;
import io.datakernel.launcher.Launcher;
import io.datakernel.remotefs.RemoteFsServer;
import io.datakernel.service.ServiceGraphModule;
import io.datakernel.trigger.TriggerRegistry;
import io.datakernel.trigger.TriggersModule;
import io.datakernel.util.guice.OptionalDependency;
import io.datakernel.util.guice.SimpleModule;

import java.net.InetSocketAddress;
import java.util.Collection;
import java.util.concurrent.ExecutorService;

import static com.google.inject.util.Modules.override;
import static io.datakernel.config.ConfigConverters.ofInetSocketAddress;
import static io.datakernel.config.ConfigConverters.ofPath;
import static io.datakernel.config.Initializers.*;
import static java.lang.Boolean.parseBoolean;
import static java.util.Arrays.asList;
import static java.util.Collections.emptyList;
import static java.util.Collections.singletonList;

public abstract class RemoteFsServerLauncher extends Launcher {
	public static final String EAGER_SINGLETONS_MODE = "eagerSingletonsMode";
	public static final String PROPERTIES_FILE = "remotefs-server.properties";

	@Inject
	RemoteFsServer remoteFsServer;

	@Override
	protected final Collection<Module> getModules() {
		return singletonList(override(getBaseModules()).with(getOverrideModules()));
	}

	private Collection<Module> getBaseModules() {
		return asList(
				ServiceGraphModule.defaultInstance(),
				JmxModule.create(),
				TriggersModule.defaultInstance(),
				ConfigModule.create(() ->
						Config.create()
								.with("remotefs.listenAddresses", Config.ofValue(ofInetSocketAddress(), new InetSocketAddress(8080)))
								.override(Config.ofProperties(PROPERTIES_FILE, true))
								.override(Config.ofProperties(System.getProperties()).getChild("config")))
						.printEffectiveConfig(),
				new SimpleModule() {
					@Provides
					@Singleton
					public Eventloop provide(Config config,
					                         OptionalDependency<ThrottlingController> maybeThrottlingController,
					                         TriggerRegistry triggerRegistry) {
						return Eventloop.create()
								.initialize(ofEventloop(config.getChild("eventloop")))
								.initialize(ofEventloopTriggers(triggerRegistry, config.getChild("triggers.eventloop")))
								.initialize(eventloop -> maybeThrottlingController.ifPresent(eventloop::withThrottlingController));
					}

					@Provides
					@Singleton
					RemoteFsServer remoteFsServer(Eventloop eventloop, ExecutorService executor, TriggerRegistry triggerRegistry,
					                              Config config) {
						return RemoteFsServer.create(eventloop, executor, config.get(ofPath(), "remotefs.path"))
								.initialize(ofRemoteFsServer(config.getChild("remotefs")))
								.initialize(ofRemoteFsServerTriggers(triggerRegistry, config.getChild("triggers.remotefs")));
					}

					@Provides
					@Singleton
					public ExecutorService provide(Config config) {
						return ConfigConverters.getExecutor(config.getChild("remotefs.executor"));
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
		launcher.launch(parseBoolean(System.getProperty(EAGER_SINGLETONS_MODE)), args);
	}
}
