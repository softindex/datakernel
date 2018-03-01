package io.datakernel.rpc.boot;

import com.google.inject.Inject;
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
import io.datakernel.rpc.server.RpcServer;
import io.datakernel.service.ServiceGraphModule;
import io.datakernel.util.guice.SimpleModule;

import java.util.Collection;

import static com.google.inject.util.Modules.combine;
import static com.google.inject.util.Modules.override;
import static io.datakernel.config.ConfigConverters.*;
import static io.datakernel.rpc.server.RpcServer.DEFAULT_PACKET_SIZE;
import static io.datakernel.rpc.server.RpcServer.MAX_PACKET_SIZE;
import static java.lang.Boolean.parseBoolean;
import static java.util.Arrays.asList;
import static java.util.Collections.emptyList;
import static java.util.Collections.singletonList;

@SuppressWarnings("SimplifiableConditionalExpression")
public abstract class RpcServerLauncher extends Launcher {
	public static final String PRODUCTION_MODE = "production";
	public static final String PROPERTIES_FILE = "rpc-server.properties";
	public static final String PROPERTIES_FILE_EFFECTIVE = "rpc-server.effective.properties";
	public static final String BUSINESS_MODULE_PROP = "businessLogicModule";

	@Inject
	RpcServer rpcServer;

	@Override
	protected final Collection<Module> getModules() {
		return asList(
				override(getBaseModules()).with(getOverrideModules()),
				combine(getBusinessLogicModules()));
	}

	protected final Collection<Module> getBaseModules() {
		return asList(
				ServiceGraphModule.defaultInstance(),
				JmxModule.create(),
				ConfigsModule.create(PropertiesConfig.ofProperties(PROPERTIES_FILE, true))
						.saveEffectiveConfigTo(PROPERTIES_FILE_EFFECTIVE),
				EventloopModule.create(),
				new SimpleModule() {
					@Provides
					@Singleton
					RpcServer provideRpcServer(Config config, Eventloop eventloop, RpcServerBusinessLogic businessLogic) {
						RpcServer server = RpcServer.create(eventloop)
								.initialize(config.get(ofAbstractServerInitializer(8080), "rpc.server"))
								.withMessageTypes(businessLogic.messageTypes)
								.withStreamProtocol(
										config.get(ofMemSize(), "rpc.streamProtocol.defaultPacketSize", DEFAULT_PACKET_SIZE),
										config.get(ofMemSize(), "rpc.streamProtocol.maxPacketSize", MAX_PACKET_SIZE),
										config.get(ofBoolean(), "rpc.streamProtocol.compression", false))
								.withFlushDelay(config.get(ofInteger(), "rpc.flushDelay", 0));
						//noinspection unchecked, ConstantConditions
						businessLogic.handlers.forEach((cls, handler) -> server.withHandler((Class) cls, null, handler));
						return server;
					}
				}
		);
	}

	protected Collection<Module> getOverrideModules() {
		return emptyList();
	}

	protected abstract Collection<Module> getBusinessLogicModules();

	@Override
	protected void run() throws Exception {
		awaitShutdown();
	}

	public static void main(String[] args) throws Exception {
		String businessLogicModuleName = System.getProperty(BUSINESS_MODULE_PROP);
		Module businessLogicModule = businessLogicModuleName != null ?
				(Module) Class.forName(businessLogicModuleName).newInstance() :
				DemoRpcBusinessLogicModule.create();

		Launcher launcher = new RpcServerLauncher() {
			@Override
			protected Collection<Module> getBusinessLogicModules() {
				return singletonList(businessLogicModule);
			}
		};
		launcher.launch(parseBoolean(System.getProperty(PRODUCTION_MODE)), args);
	}
}
