import io.datakernel.di.annotation.Inject;
import io.datakernel.di.module.Module;
import io.datakernel.launcher.Launcher;
import io.datakernel.service.Service;
import io.datakernel.service.ServiceGraphModule;

import java.util.concurrent.CompletableFuture;

@SuppressWarnings("unused")
//[START EXAMPLE]
public class SimpleServiceExample extends Launcher {
	public static void main(String[] args) throws Exception {
		SimpleServiceExample example = new SimpleServiceExample();
		example.launch(args);
	}

	@Inject CustomService customService;

	@Override
	protected Module getModule() {
		return ServiceGraphModule.create();
	}

	@Inject
	private static class CustomService implements Service {
		@Override
		public CompletableFuture<?> start() {
			System.out.println("|SERVICE STARTING|");
			return CompletableFuture.completedFuture(null);
		}

		@Override
		public CompletableFuture<?> stop() {
			System.out.println("|SERVICE STOPPING|");
			return CompletableFuture.completedFuture(null);
		}
	}

	@Override
	protected void run() {
		System.out.println("|RUNNING|");
	}
}
//[END EXAMPLE]
