import io.datakernel.di.Injector;
import io.datakernel.di.annotation.Provides;
import io.datakernel.di.module.AbstractModule;
import io.datakernel.eventloop.Eventloop;
import io.datakernel.service.ServiceGraph;
import io.datakernel.service.ServiceGraphModule;

import java.util.concurrent.ExecutionException;

//[START EXAMPLE]
public final class ServiceGraphModuleExample extends AbstractModule {
	@Provides
	Eventloop eventloop() {
		return Eventloop.create();
	}

	public static void main(String[] args) throws ExecutionException, InterruptedException {
		Injector injector = Injector.of(ServiceGraphModule.create(), new ServiceGraphModuleExample());
		Eventloop eventloop = injector.getInstance(Eventloop.class);

		eventloop.execute(() -> System.out.println("\nHello World\n"));

		ServiceGraph serviceGraph = injector.getInstance(ServiceGraph.class);
		try {
			serviceGraph.startFuture().get();
		} finally {
			serviceGraph.stopFuture().get();
		}
	}
}
//[END EXAMPLE]
