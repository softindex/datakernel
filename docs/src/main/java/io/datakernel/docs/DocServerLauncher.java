package io.datakernel.docs;

import io.datakernel.config.Config;
import io.datakernel.di.annotation.Export;
import io.datakernel.di.annotation.Named;
import io.datakernel.di.annotation.Provides;
import io.datakernel.di.core.Binding;
import io.datakernel.di.core.Key;
import io.datakernel.di.module.Module;
import io.datakernel.docs.module.AutomationModule;
import io.datakernel.docs.module.ServletsModule;
import io.datakernel.launchers.http.HttpServerLauncher;

import java.util.concurrent.Executor;

import static io.datakernel.config.Config.ofProperties;
import static io.datakernel.di.module.Modules.combine;
import static java.lang.System.getProperties;
import static java.util.concurrent.Executors.newCachedThreadPool;

public class DocServerLauncher extends HttpServerLauncher {
	@Export
	@Provides
	@Named("properties")
	Config config() {
		return ofProperties("app.properties")
				.overrideWith(ofProperties(getProperties())
						.getChild("config"));
	}

	@Export
	@Provides
	Executor executor() {
		return newCachedThreadPool();
	}

	@Override
	protected Module getBusinessLogicModule() {
		return combine(
				new AutomationModule()
						.rebindImport(Key.of(Config.class), Binding.to(conf -> conf.getChild("servlet"), Key.of(Config.class, "properties"))),
				new ServletsModule()
						.rebindImport(Key.of(Config.class), Key.of(Config.class, "properties")));
	}

	public static void main(String[] args) throws Exception {
		HttpServerLauncher launcher = new DocServerLauncher();
		launcher.launch(args);
	}
}
