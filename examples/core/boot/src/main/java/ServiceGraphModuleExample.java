import io.datakernel.di.annotation.Provides;
import io.datakernel.di.core.Injector;
import io.datakernel.di.module.AbstractModule;
import io.datakernel.eventloop.Eventloop;
import io.datakernel.logger.LoggerConfigurer;
import io.datakernel.service.ServiceGraph;
import io.datakernel.service.ServiceGraphModule;

import java.util.concurrent.ExecutionException;

public final class ServiceGraphModuleExample extends AbstractModule {
	static {
		LoggerConfigurer.enableLogging();
	}
	@Provides
	Eventloop eventloop() {
		return Eventloop.create();
	}

	public static void main(String[] args) throws ExecutionException, InterruptedException {
		Injector injector = Injector.of(ServiceGraphModule.defaultInstance(), new ServiceGraphModuleExample());
		Eventloop eventloop = injector.getInstance(Eventloop.class);

		eventloop.execute(() -> System.out.println("Hello World"));

		ServiceGraph serviceGraph = injector.getInstance(ServiceGraph.class);
		try {
			serviceGraph.startFuture().get();
		} finally {
			serviceGraph.stopFuture().get();
		}
	}
}
