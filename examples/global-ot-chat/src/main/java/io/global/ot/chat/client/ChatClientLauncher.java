package io.global.ot.chat.client;

import com.google.inject.Inject;
import com.google.inject.Module;
import io.datakernel.config.Config;
import io.datakernel.config.ConfigModule;
import io.datakernel.http.AsyncHttpServer;
import io.datakernel.launcher.Launcher;
import io.datakernel.service.ServiceGraphModule;

import java.util.Collection;

import static io.datakernel.config.Config.ofProperties;
import static java.lang.Boolean.parseBoolean;
import static java.util.Arrays.asList;

public class ChatClientLauncher extends Launcher {
	public static final String EAGER_SINGLETONS_MODE = "eagerSingletonsMode";
	public static final String PROPERTIES_FILE = "client.properties";

	@Inject
	AsyncHttpServer server;

	@Override
	protected Collection<Module> getModules() {
		return asList(
				ServiceGraphModule.defaultInstance(),
				ConfigModule.create(() ->
						Config.ofProperties(PROPERTIES_FILE)
								.override(ofProperties(System.getProperties()).getChild("config")))
						.printEffectiveConfig(),
				new ChatClientModule()
		);
	}

	@Override
	protected void run() throws Exception {
		awaitShutdown();
	}

	public static void main(String[] args) throws Exception {
		new ChatClientLauncher().launch(parseBoolean(System.getProperty(EAGER_SINGLETONS_MODE)), args);
	}
}
