package io.datakernel.http.boot;

import com.google.inject.Module;
import com.google.inject.Stage;
import io.datakernel.config.ConfigsModule;
import io.datakernel.config.impl.PropertiesConfig;
import io.datakernel.jmx.JmxModule;
import io.datakernel.launcher.Launcher;
import io.datakernel.launcher.modules.PrimaryEventloopModule;
import io.datakernel.launcher.modules.WorkerEventloopModule;
import io.datakernel.launcher.modules.WorkerPoolModule;
import io.datakernel.service.ServiceGraphModule;

public class HttpWorkerServerLauncher extends Launcher {
	public static final String PROP_FILE = "http.properties";
	public static final String CONFIG_PROP = "http.config";
	public static final String BUSINESS_MODULE_PROP = "http.businessLogicModule";

	public HttpWorkerServerLauncher() {
		super(Stage.PRODUCTION,
				ServiceGraphModule.defaultInstance(),
				JmxModule.create(),
				ConfigsModule.create(PropertiesConfig.ofProperties(PROP_FILE, true))
						.saveEffectiveConfigTo(PROP_FILE),
				WorkerPoolModule.create(),
				PrimaryEventloopModule.create(),
				PrimaryHttpServerModule.create(),
				WorkerEventloopModule.create(),
				WorkerHttpServerModule.create());
	}

	@Override
	protected void run() throws Exception {
		awaitShutdown();
	}

	public static void main(String[] args) throws Exception {
		Launcher launcher = new HttpWorkerServerLauncher();
		String moduleName = System.getProperty(BUSINESS_MODULE_PROP);
		launcher.addModule(moduleName == null ?
				HelloWorldWorkerServletModule.create() :
				(Module) Class.forName(moduleName).newInstance());
		String config = System.getProperty(CONFIG_PROP);
		if (config != null) {
			launcher.addOverride(ConfigsModule.create(PropertiesConfig.ofProperties(config))
					.saveEffectiveConfigTo(config));
		}
		launcher.launch(args);
	}
}
