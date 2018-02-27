package io.datakernel.remotefs.boot;

import com.google.inject.Stage;
import io.datakernel.config.ConfigsModule;
import io.datakernel.config.impl.PropertiesConfig;
import io.datakernel.jmx.JmxModule;
import io.datakernel.launcher.Launcher;
import io.datakernel.launcher.modules.EventloopModule;
import io.datakernel.launcher.modules.ExecutorServiceModule;
import io.datakernel.service.ServiceGraphModule;

public class RemoteFsServerLauncher extends Launcher {
	public static final String PROP_FILE = "remotefs.properties";
	public static final String CONFIG_PROP = "remotefs.config";

	public RemoteFsServerLauncher() {
		super(Stage.PRODUCTION,
				ServiceGraphModule.defaultInstance(),
				JmxModule.create(),
				ConfigsModule.create(PropertiesConfig.ofProperties(PROP_FILE, true))
						.saveEffectiveConfigTo(PROP_FILE),
				EventloopModule.create(),
				ExecutorServiceModule.create(),
				RemoteFsServerModule.create());
	}

	@Override
	protected void run() throws Exception {
		awaitShutdown();
	}

	public static void main(String[] args) throws Exception {
		Launcher launcher = new RemoteFsServerLauncher();
		String config = System.getProperty(CONFIG_PROP);
		if (config != null) {
			launcher.addOverride(ConfigsModule.create(PropertiesConfig.ofProperties(config))
					.saveEffectiveConfigTo(config));
		}
		launcher.launch(args);
	}
}
