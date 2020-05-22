import io.datakernel.async.service.EventloopService;
import io.datakernel.di.annotation.Eager;
import io.datakernel.di.annotation.Provides;
import io.datakernel.di.module.Module;
import io.datakernel.eventloop.Eventloop;
import io.datakernel.launcher.Launcher;
import io.datakernel.promise.Promise;
import io.datakernel.promise.Promises;
import io.datakernel.service.ServiceGraphModule;
import org.jetbrains.annotations.NotNull;

import java.time.Duration;

//[START EXAMPLE]
public class EventloopServiceExample extends Launcher {

	@Provides
	Eventloop eventloop() {
		return Eventloop.create();
	}

	@Provides
	@Eager
	CustomEventloopService customEventloopService(Eventloop eventloop) {
		return new CustomEventloopService(eventloop);
	}

	@Override
	protected Module getModule() {
		return ServiceGraphModule.create();
	}

	@Override
	protected void run() {
		System.out.println("|RUNNING|");
	}

	private static final class CustomEventloopService implements EventloopService {
		private final Eventloop eventloop;

		public CustomEventloopService(Eventloop eventloop) {
			this.eventloop = eventloop;
		}

		@Override
		public @NotNull Eventloop getEventloop() {
			return eventloop;
		}

		@Override
		public @NotNull Promise<?> start() {
			System.out.println("|CUSTOM EVENTLOOP SERVICE STARTING|");
			return Promises.delay(Duration.ofMillis(10))
					.whenResult(() -> System.out.println("|CUSTOM EVENTLOOP SERVICE STARTED|"));
		}

		@Override
		public @NotNull Promise<?> stop() {
			System.out.println("|CUSTOM EVENTLOOP SERVICE STOPPING|");
			return Promises.delay(Duration.ofMillis(10))
					.whenResult(() -> System.out.println("|CUSTOM EVENTLOOP SERVICE STOPPED|"));
		}
	}

	public static void main(String[] args) throws Exception {
		EventloopServiceExample example = new EventloopServiceExample();
		example.launch(args);
	}
}
//[END EXAMPLE]
