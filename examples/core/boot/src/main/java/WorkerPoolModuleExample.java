import io.datakernel.di.annotation.Provides;
import io.datakernel.di.core.Injector;
import io.datakernel.di.module.AbstractModule;
import io.datakernel.worker.*;

//[START EXAMPLE]
public final class WorkerPoolModuleExample extends AbstractModule {
	@Provides
	WorkerPool workerPool(WorkerPools workerPools) {
		return workerPools.createPool(4);
	}

	@Provides
	@Worker
	String string(@WorkerId int workerId) {
		return "Hello from worker #" + workerId;
	}

	public static void main(String[] args) {
		Injector injector = Injector.of(new WorkerPoolModule(), new WorkerPoolModuleExample());
		WorkerPool workerPool = injector.getInstance(WorkerPool.class);
		WorkerPool.Instances<String> strings = workerPool.getInstances(String.class);
		strings.forEach(System.out::println);
	}
}
//[END EXAMPLE]
