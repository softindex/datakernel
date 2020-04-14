package io.datakernel.launchers.dataflow;

import io.datakernel.config.Config;
import io.datakernel.config.ConfigModule;
import io.datakernel.dataflow.server.DataflowEnvironment;
import io.datakernel.dataflow.server.DataflowSerialization;
import io.datakernel.dataflow.server.DataflowServer;
import io.datakernel.di.annotation.Inject;
import io.datakernel.di.annotation.Optional;
import io.datakernel.di.annotation.Provides;
import io.datakernel.di.core.Key;
import io.datakernel.di.module.AbstractModule;
import io.datakernel.di.module.Module;
import io.datakernel.eventloop.Eventloop;
import io.datakernel.eventloop.ThrottlingController;
import io.datakernel.jmx.JmxModule;
import io.datakernel.launcher.Launcher;
import io.datakernel.launcher.OnStart;
import io.datakernel.service.ServiceGraphModule;

import java.util.concurrent.CompletionStage;

import static io.datakernel.di.module.Modules.combine;
import static io.datakernel.launchers.initializers.Initializers.*;

public abstract class DataflowServerLauncher extends Launcher {
	public static final String PROPERTIES_FILE = "dataflow-server.properties";
	public static final String BUSINESS_MODULE_PROP = "businessLogicModule";

	@Inject
	DataflowServer dataflowServer;

	@Provides
	Eventloop eventloop(Config config, @Optional ThrottlingController throttlingController) {
		return Eventloop.create()
				.initialize(ofEventloop(config.getChild("eventloop")))
				.initialize(eventloop -> eventloop.withInspector(throttlingController));
	}

	@Provides
	DataflowServer server(Eventloop eventloop, DataflowEnvironment environment, Config config) {
		return new DataflowServer(eventloop, environment)
				.initialize(ofAbstractServer(config.getChild("dataflow.server")))
				.initialize(s -> s.withSocketSettings(s.getSocketSettings().withTcpNoDelay(true)));
	}

	@Provides
	Config config() {
		return Config.create()
				.overrideWith(Config.ofClassPathProperties(PROPERTIES_FILE, true))
				.overrideWith(Config.ofProperties(System.getProperties()).getChild("config"));
	}

	@Override
	protected final Module getModule() {
		return combine(
				ServiceGraphModule.create()
						.initialize(ofAsyncComponents()),
				JmxModule.create(),
				ConfigModule.create()
						.printEffectiveConfig()
						.rebindImport(new Key<CompletionStage<Void>>() {}, new Key<CompletionStage<Void>>(OnStart.class) {}),
				getBusinessLogicModule()
		);
	}

	/**
	 * Override this method to supply your launcher business logic.
	 */
	protected Module getBusinessLogicModule() {
		return Module.empty();
	}

	@Override
	protected void run() throws Exception {
		awaitShutdown();
	}

	public static void main(String[] args) throws Exception {
		String businessLogicModuleName = System.getProperty(BUSINESS_MODULE_PROP);

		Module businessLogicModule = businessLogicModuleName != null ?
				(Module) Class.forName(businessLogicModuleName).newInstance() :
				new AbstractModule() {
					@Provides
					public DataflowEnvironment environment() {
						return DataflowEnvironment.create()
								.setInstance(DataflowSerialization.class, DataflowSerialization.create());
					}
				};

		Launcher launcher = new DataflowServerLauncher() {
			@Override
			protected Module getBusinessLogicModule() {
				return businessLogicModule;
			}
		};

		launcher.launch(args);
	}
}
