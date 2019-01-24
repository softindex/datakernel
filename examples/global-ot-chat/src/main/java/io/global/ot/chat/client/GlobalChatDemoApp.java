package io.global.ot.chat.client;

import com.google.inject.Inject;
import com.google.inject.Module;
import com.google.inject.name.Named;
import io.datakernel.async.EventloopTaskScheduler;
import io.datakernel.config.Config;
import io.datakernel.config.ConfigModule;
import io.datakernel.http.AsyncHttpServer;
import io.datakernel.launcher.Launcher;
import io.datakernel.service.ServiceGraphModule;

import java.util.Collection;

import static io.datakernel.config.Config.ofProperties;
import static java.lang.Boolean.parseBoolean;
import static java.util.Arrays.asList;

public final class GlobalChatDemoApp extends Launcher {
	public static final String EAGER_SINGLETONS_MODE = "eagerSingletonsMode";
	public static final String PROPERTIES_FILE = "client.properties";
	public static final String CREDENTIALS_FILE = "credentials.properties";
	public static final String DEFAULT_LISTEN_ADDRESSES = "*:8080";
	public static final String DEFAULT_MERGE_INTERVAL = "10 seconds";
	public static final String DEFAULT_MERGE_TYPE = "interval";

	@Inject
	AsyncHttpServer server;

	@Inject
	@Named("Merge")
	EventloopTaskScheduler mergeScheduler;

	@Override
	protected Collection<Module> getModules() {
		return asList(
				ServiceGraphModule.defaultInstance(),
				ConfigModule.create(() ->
						Config.create()
								.with("merge.schedule.type", DEFAULT_MERGE_TYPE)
								.with("merge.schedule.value", DEFAULT_MERGE_INTERVAL)
								.with("http.listenAddresses", DEFAULT_LISTEN_ADDRESSES)
								.override(Config.ofProperties(PROPERTIES_FILE, true)
										.combine(Config.ofProperties(CREDENTIALS_FILE, true)))
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
		new GlobalChatDemoApp().launch(parseBoolean(System.getProperty(EAGER_SINGLETONS_MODE)), args);
	}
}
