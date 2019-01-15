package io.global.ot.chat.gateway;

import com.google.inject.*;
import io.datakernel.async.EventloopTaskScheduler;
import io.datakernel.codec.StructuredCodec;
import io.datakernel.config.Config;
import io.datakernel.config.ConfigModule;
import io.datakernel.http.AsyncHttpServer;
import io.datakernel.launcher.Launcher;
import io.datakernel.service.ServiceGraphModule;
import io.global.ot.chat.common.Operation;

import java.util.Collection;

import static com.google.inject.util.Modules.combine;
import static io.datakernel.config.Config.ofProperties;
import static java.util.Arrays.asList;

public abstract class GatewayLauncher extends Launcher {
	public static final String EAGER_SINGLETONS_MODE = "eagerSingletonsMode";
	public static final String PROPERTIES_FILE = "gateway.properties";

	@Inject
	AsyncHttpServer server;

	@Inject
	EventloopTaskScheduler merger;

	@Override
	protected final Collection<Module> getModules() {
		return asList(
				ServiceGraphModule.defaultInstance(),
				ConfigModule.create(() ->
						Config.ofProperties(PROPERTIES_FILE)
								.override(ofProperties(System.getProperties()).getChild("config")))
						.printEffectiveConfig(),
				combine(new GatewayModule(), new AbstractModule() {
					@Provides
					@Singleton
					StructuredCodec<Operation> provideCodec() {
						return getOperationCodec();
					}
				})
		);
	}

	protected abstract StructuredCodec<Operation> getOperationCodec();

	@Override
	protected void run() throws Exception {
		awaitShutdown();
	}
}
