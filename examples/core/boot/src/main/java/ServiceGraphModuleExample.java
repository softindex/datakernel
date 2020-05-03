import io.datakernel.di.Injector;
import io.datakernel.di.annotation.Provides;
import io.datakernel.di.module.AbstractModule;
import io.datakernel.eventloop.Eventloop;
import io.datakernel.service.ServiceGraph;
import io.datakernel.service.ServiceGraphModule;

import java.util.concurrent.ExecutionException;

import static io.datakernel.service.ServiceAdapters.forEventloop;

//[START EXAMPLE]
public final class ServiceGraphModuleExample extends AbstractModule {
	@Provides
	Eventloop eventloop() {
		return Eventloop.create();
	}

	public static void main(String[] args) throws ExecutionException, InterruptedException {
		ServiceGraphModule serviceGraphModule = ServiceGraphModule.create()
				.register(Eventloop.class, forEventloop());
		Injector injector = Injector.of(serviceGraphModule, new ServiceGraphModuleExample());
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
//[END EXAMPLE]
