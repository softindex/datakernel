import io.datakernel.async.MaterializedPromise;
import io.datakernel.async.Promise;
import io.datakernel.di.annotation.Inject;
import io.datakernel.di.annotation.Provides;
import io.datakernel.di.module.Module;
import io.datakernel.eventloop.Eventloop;
import io.datakernel.eventloop.EventloopService;
import io.datakernel.launcher.Launcher;
import io.datakernel.service.Service;
import io.datakernel.service.ServiceGraphModule;
import org.jetbrains.annotations.NotNull;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;


@SuppressWarnings("unused")
public class AdvancedServiceExample extends Launcher {
	@Inject DBService dbService;
	@Inject AuthService authService;
	@Inject EmailService emailService;

	@Provides
	DBService dbService() {
		return new DBService();
	}

	@Provides
	EmailService emailService(DBService dbService, AuthService authService) {
		return new EmailService(dbService, authService);
	}

	@Provides
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
		return ServiceGraphModule.defaultInstance();
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
		public @NotNull MaterializedPromise<?> start() {
			System.out.println("AuthService starting");
			return Promise.ofBlockingRunnable(executor,
					() -> System.out.println("AuthService started"))
					.materialize();
		}

		@Override
		public @NotNull MaterializedPromise<?> stop() {
			return Promise.ofBlockingRunnable(executor,
					() -> System.out.println("AuthService stopped"))
					.materialize();
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
			return CompletableFuture.runAsync(() -> {
				System.out.println("EmailService is stopped");
			});
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
