import io.datakernel.async.service.EventloopService;
import io.datakernel.di.annotation.Eager;
import io.datakernel.di.annotation.Provides;
import io.datakernel.di.module.Module;
import io.datakernel.eventloop.Eventloop;
import io.datakernel.launcher.Launcher;
import io.datakernel.promise.Promise;
import io.datakernel.service.ServiceGraphModule;
import org.jetbrains.annotations.NotNull;

import java.util.concurrent.Executor;
import java.util.concurrent.Executors;

//[START EXAMPLE]
public class EventloopServiceExample extends Launcher {

	@Provides
	Eventloop eventloop() {
		return Eventloop.create();
	}

	@Provides
	Executor executor() {
		return Executors.newCachedThreadPool();
	}

	@Provides
	@Eager
	CustomEventloopService customEventloopService(Eventloop eventloop, Executor executor) {
		return new CustomEventloopService(eventloop, executor);
	}

	@Override
	protected Module getModule() {
		return ServiceGraphModule.create();
	}

	@Override
	protected void run() { }

	private static final class CustomEventloopService implements EventloopService {
		private final Executor executor;
		private final Eventloop eventloop;

		public CustomEventloopService(Eventloop eventloop, Executor executor) {
			this.executor = executor;
			this.eventloop = eventloop;
		}

		@Override
		public @NotNull Eventloop getEventloop() {
			return eventloop;
		}

		@Override
		public @NotNull Promise<?> start() {
			System.out.println(String.format("|%s|", "Eventloop-Service starting".toUpperCase()));
			return Promise.ofBlockingRunnable(executor,
					() -> System.out.println(String.format("|%s|", "Eventloop-Service started".toUpperCase())));
		}

		@Override
		public @NotNull Promise<?> stop() {
			System.out.println(String.format("|%s|", "Eventloop-Service stopping".toUpperCase()));
			return Promise.ofBlockingRunnable(executor,
					() -> System.out.println(String.format("|%s|", "Eventloop-Service stopped".toUpperCase())));
		}
	}

	public static void main(String[] args) throws Exception {
		EventloopServiceExample example = new EventloopServiceExample();
		example.launch(args);
	}
}
//[END EXAMPLE]
