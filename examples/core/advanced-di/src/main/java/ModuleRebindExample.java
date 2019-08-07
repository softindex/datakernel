import io.datakernel.config.Config;
import io.datakernel.config.ConfigModule;
import io.datakernel.di.annotation.Inject;
import io.datakernel.di.annotation.Provides;
import io.datakernel.di.core.Key;
import io.datakernel.di.module.Module;
import io.datakernel.launcher.Launcher;
import io.datakernel.launcher.OnRun;

import java.util.concurrent.CompletionStage;

//[START EXAMPLE]
public class ModuleRebindExample extends Launcher {

	@Inject Config config;

	@Provides
	Config config() {
		return Config.create()
				.with("counter", "10")
				.with("message", "Launcher is working");
	}
	@Override
	protected Module getModule() {
		return ConfigModule.create()
				.printEffectiveConfig()
				.rebindImport(new Key<CompletionStage<Void>>() {}, new Key<CompletionStage<Void>>(OnRun.class) {});
	}
	@Override
	protected void run() {
		int counter = Integer.parseInt(config.get("counter"));
		for(int i = 0; i < counter; ++i) {
			System.out.println(config.get("message"));
		}
	}
	public static void main(String[] args) throws Exception {
		ModuleRebindExample example = new ModuleRebindExample();
		example.launch(args);
	}
	//[END EXAMPLE]
}
