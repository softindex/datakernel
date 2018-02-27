package io.datakernel.rpc.boot;

import com.google.inject.Module;
import com.google.inject.Stage;
import io.datakernel.config.ConfigsModule;
import io.datakernel.config.impl.PropertiesConfig;
import io.datakernel.jmx.JmxModule;
import io.datakernel.launcher.Launcher;
import io.datakernel.launcher.modules.EventloopModule;
import io.datakernel.service.ServiceGraphModule;

public class RpcServerLauncher extends Launcher {
	public static final String PROP_FILE = "rpc.properties";
	public static final String CONFIG_PROP = "rpc.config";
	public static final String BUSINESS_MODULE_PROP = "rpc.businessLogicModule";

	public RpcServerLauncher() {
		super(Stage.PRODUCTION,
				ServiceGraphModule.defaultInstance(),
				JmxModule.create(),
				ConfigsModule.create(PropertiesConfig.ofProperties(PROP_FILE, true))
						.saveEffectiveConfigTo(PROP_FILE),
				EventloopModule.create(),
				RpcServerModule.create());
	}

	@Override
	protected void run() throws Exception {
		awaitShutdown();
	}

	public static void main(String[] args) throws Exception {
		Launcher launcher = new RpcServerLauncher();
		String moduleName = System.getProperty(BUSINESS_MODULE_PROP);
		launcher.addModule(moduleName == null ?
				DemoRpcBusinessLogicModule.create() :
				(Module) Class.forName(moduleName).newInstance());
		String config = System.getProperty(CONFIG_PROP);
		if (config != null) {
			launcher.addOverride(ConfigsModule.create(PropertiesConfig.ofProperties(config))
					.saveEffectiveConfigTo(config));
		}
		launcher.launch(args);
	}
}
