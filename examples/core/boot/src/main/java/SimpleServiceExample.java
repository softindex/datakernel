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
			System.out.println(String.format("|%s|", "Service starting".toUpperCase()));
			return CompletableFuture.completedFuture(null)
					.whenCompleteAsync(($1, $2) ->
							System.out.println(String.format("|%s|", "Service started".toUpperCase())));
		}

		@Override
		public CompletableFuture<?> stop() {
			System.out.println(String.format("|%s|", "Service stopping".toUpperCase()));
			return CompletableFuture.completedFuture(null)
					.whenCompleteAsync(($1, $2) ->
							System.out.println(String.format("|%s|", "Service stopped".toUpperCase())));
		}
	}

	@Override
	protected void run() {
		System.out.println("RUN");
	}
}
//[END EXAMPLE]
