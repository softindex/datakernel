package io.datakernel.rpc.boot;

import com.google.inject.Inject;
import com.google.inject.Module;
import com.google.inject.Provides;
import com.google.inject.Singleton;
import io.datakernel.config.Config;
import io.datakernel.config.ConfigModule;
import io.datakernel.eventloop.Eventloop;
import io.datakernel.jmx.JmxModule;
import io.datakernel.launcher.Launcher;
import io.datakernel.launcher.modules.EventloopModule;
import io.datakernel.rpc.server.RpcServer;
import io.datakernel.service.ServiceGraphModule;
import io.datakernel.trigger.TriggersModule;
import io.datakernel.util.Initializer;
import io.datakernel.util.guice.SimpleModule;

import java.util.Collection;

import static com.google.inject.util.Modules.combine;
import static com.google.inject.util.Modules.override;
import static io.datakernel.rpc.boot.ConfigInitializers.ofRpcServer;
import static java.lang.Boolean.parseBoolean;
import static java.util.Arrays.asList;
import static java.util.Collections.emptyList;
import static java.util.Collections.singletonList;

@SuppressWarnings("SimplifiableConditionalExpression")
public abstract class RpcServerLauncher extends Launcher {
	public static final String EAGER_SINGLETONS_MODE = "eagerSingletonsMode";
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

	private Collection<Module> getBaseModules() {
		return asList(
				ServiceGraphModule.defaultInstance(),
				JmxModule.create(),
				TriggersModule.defaultInstance(),
				ConfigModule.create(() ->
						Config.ofProperties(PROPERTIES_FILE, true)
								.override(Config.ofProperties(System.getProperties()).getChild("config")))
						.saveEffectiveConfigTo(PROPERTIES_FILE_EFFECTIVE),
				EventloopModule.create(),
				new SimpleModule() {
					@Provides
					@Singleton
					RpcServer provideRpcServer(Config config, Eventloop eventloop, Initializer<RpcServer> rpcServerInitializer) {
						return RpcServer.create(eventloop)
								.initialize(ofRpcServer(config))
								.initialize(rpcServerInitializer);
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
				new DemoRpcBusinessLogicModule();

		Launcher launcher = new RpcServerLauncher() {
			@Override
			protected Collection<Module> getBusinessLogicModules() {
				return singletonList(businessLogicModule);
			}
		};
		launcher.launch(parseBoolean(System.getProperty(EAGER_SINGLETONS_MODE)), args);
	}
}
