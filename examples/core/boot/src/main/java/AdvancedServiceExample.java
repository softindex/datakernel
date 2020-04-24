import io.datakernel.async.service.EventloopService;
import io.datakernel.di.annotation.Eager;
import io.datakernel.di.annotation.Provides;
import io.datakernel.di.module.Module;
import io.datakernel.eventloop.Eventloop;
import io.datakernel.launcher.Launcher;
import io.datakernel.promise.Promise;
import io.datakernel.service.Service;
import io.datakernel.service.ServiceGraphModule;
import org.jetbrains.annotations.NotNull;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;

@SuppressWarnings("unused")
//[START EXAMPLE]
public class AdvancedServiceExample extends Launcher {
	@Provides
	@Eager
	DBService dbService() {
		return new DBService();
	}

	@Provides
	@Eager
	EmailService emailService(DBService dbService, AuthService authService) {
		return new EmailService(dbService, authService);
	}

	@Provides
	@Eager
	AuthService authService(Eventloop eventloop, Executor executor, DBService dbService) {
		return new AuthService(eventloop, executor, dbService);
	}

	@Provides
	Eventloop eventloop() {
		return Eventloop.create().withCurrentThread();
	}

	@Provides
	Executor executor() {
		return Executors.newCachedThreadPool();
	}

	@Override
	protected Module getModule() {
		return ServiceGraphModule.create();
	}

	@SuppressWarnings("FieldCanBeLocal")
	private static class AuthService implements EventloopService {
		private final Eventloop eventloop;
		private final Executor executor;
		private final DBService dbService;

		public AuthService(Eventloop eventloop, Executor executor, DBService dbService) {
			this.executor = executor;
			this.eventloop = eventloop;
			this.dbService = dbService;
		}

		@Override
		public @NotNull Eventloop getEventloop() {
			return eventloop;
		}

		@Override
		public @NotNull Promise<?> start() {
			System.out.println("AuthService starting");
			return Promise.ofBlockingRunnable(executor,
					() -> System.out.println("AuthService started"));
		}

		@Override
		public @NotNull Promise<?> stop() {
			return Promise.ofBlockingRunnable(executor,
					() -> System.out.println("AuthService stopped"));
		}
	}

	private static class DBService implements Service {
		@Override
		public CompletableFuture<?> start() {
			System.out.println("DBService is starting");
			return CompletableFuture.runAsync(() -> {
				try {
					Thread.sleep(1000);
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
				System.out.println("DBService is started");
			});
		}

		@Override
		public CompletableFuture<?> stop() {
			System.out.println("DBService is stopping");
			return CompletableFuture.runAsync(() -> {
				try {
					Thread.sleep(2000);
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
				System.out.println("DBService is stopped");
			});
		}
	}

	@SuppressWarnings("FieldCanBeLocal")
	private static class EmailService implements Service {
		private final DBService service;
		private final AuthService authService;

		public EmailService(DBService service, AuthService authService) {
			this.service = service;
			this.authService = authService;
		}

		@Override
		public CompletableFuture<?> start() {
			System.out.println("EmailService is starting");
			return CompletableFuture.runAsync(() -> {
				try {
					Thread.sleep(1000);
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
				System.out.println("EmailService is started");
			});
		}

		@Override
		public CompletableFuture<?> stop() {
			System.out.println("EmailService is stopping");
			return CompletableFuture.runAsync(() -> System.out.println("EmailService is stopped"));
		}
	}

	@Override
	protected void run() {
	}

	public static void main(String[] args) throws Exception {
		AdvancedServiceExample example = new AdvancedServiceExample();
		example.launch(args);
	}
}
//[END EXAMPLE]
