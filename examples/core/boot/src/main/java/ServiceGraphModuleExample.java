import io.datakernel.di.annotation.Provides;
import io.datakernel.di.core.Injector;
import io.datakernel.di.module.AbstractModule;
import io.datakernel.eventloop.Eventloop;
import io.datakernel.service.ServiceGraph;
import io.datakernel.service.ServiceGraphModule;
import org.slf4j.bridge.SLF4JBridgeHandler;

import java.util.concurrent.ExecutionException;

public final class ServiceGraphModuleExample extends AbstractModule {
	static {
		SLF4JBridgeHandler.removeHandlersForRootLogger();
		SLF4JBridgeHandler.install();
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
