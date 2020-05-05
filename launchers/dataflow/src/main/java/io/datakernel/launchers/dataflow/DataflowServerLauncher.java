package io.datakernel.launchers.dataflow;

import io.datakernel.config.Config;
import io.datakernel.config.ConfigModule;
import io.datakernel.csp.binary.ByteBufsCodec;
import io.datakernel.dataflow.DataflowClient;
import io.datakernel.dataflow.DataflowServer;
import io.datakernel.dataflow.command.DataflowCommand;
import io.datakernel.dataflow.command.DataflowResponse;
import io.datakernel.dataflow.di.BinarySerializersModule.BinarySerializers;
import io.datakernel.dataflow.di.DataflowModule;
import io.datakernel.di.Injector;
import io.datakernel.di.annotation.Inject;
import io.datakernel.di.annotation.Optional;
import io.datakernel.di.annotation.Provides;
import io.datakernel.di.module.Module;
import io.datakernel.eventloop.Eventloop;
import io.datakernel.eventloop.inspector.ThrottlingController;
import io.datakernel.jmx.JmxModule;
import io.datakernel.launcher.Launcher;
import io.datakernel.service.ServiceGraphModule;

import java.util.concurrent.Executor;

import static io.datakernel.config.converter.ConfigConverters.getExecutor;
import static io.datakernel.config.converter.ConfigConverters.ofPath;
import static io.datakernel.di.module.Modules.combine;
import static io.datakernel.launchers.initializers.Initializers.ofAbstractServer;
import static io.datakernel.launchers.initializers.Initializers.ofEventloop;

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
	Executor executor(Config config) {
		return getExecutor(config);
	}

	@Provides
	DataflowServer server(Eventloop eventloop, Config config, ByteBufsCodec<DataflowCommand, DataflowResponse> codec, BinarySerializers serializers, Injector environment) {
		return new DataflowServer(eventloop, codec, serializers, environment)
				.initialize(ofAbstractServer(config.getChild("dataflow.server")))
				.initialize(s -> s.withSocketSettings(s.getSocketSettings().withTcpNoDelay(true)));
	}

	@Provides
	DataflowClient client(Executor executor, Config config, ByteBufsCodec<DataflowResponse, DataflowCommand> codec, BinarySerializers serializers) {
		return new DataflowClient(executor, config.get(ofPath(), "dataflow.secondaryBufferPath"), codec, serializers);
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
				ServiceGraphModule.create(),
				JmxModule.create(),
				DataflowModule.create(),
				ConfigModule.create()
						.withEffectiveConfigLogger(),
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
				Module.empty();

		Launcher launcher = new DataflowServerLauncher() {
			@Override
			protected Module getBusinessLogicModule() {
				return businessLogicModule;
			}
		};

		launcher.launch(args);
	}
}
