package io.datakernel.trigger;

import com.google.inject.*;
import com.google.inject.name.Named;
import io.datakernel.eventloop.Eventloop;
import io.datakernel.service.ServiceGraph;
import io.datakernel.service.ServiceGraphModule;
import io.datakernel.worker.Worker;
import io.datakernel.worker.WorkerPool;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import static com.google.inject.name.Names.named;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class TriggersModuleTest {
	@Rule
	public ExpectedException exception = ExpectedException.none();

	@Test
	public void testDuplicatesRejection() {
		exception.expect(IllegalArgumentException.class);
		exception.expectMessage("Cannot assign duplicate triggers");
		Injector injector = Guice.createInjector(
				ServiceGraphModule.defaultInstance(),
				TriggersModule.create()
						.with(Eventloop.class, Severity.HIGH, "test", eventloop -> TriggerResult.create())
						.with(Eventloop.class, Severity.HIGH, "test", eventloop -> TriggerResult.create())
		);
	}

	@Test
	public void testWithSeveralWorkerPools() throws Exception {
		int firstPoolSize = 10;
		int secondPoolSize = 5;
		Injector injector = Guice.createInjector(
				ServiceGraphModule.defaultInstance(),
				new AbstractModule() {
					int counter = 0;
					@Provides
					@Singleton
					@Named("first")
					WorkerPool provideFirstPool() {
						return new WorkerPool(firstPoolSize);
					}

					@Provides
					@Singleton
					@Named("second")
					WorkerPool provideSecondPool() {
						return new WorkerPool(secondPoolSize);
					}

					@Provides
					@Worker
					String provideWorker() {
						return "" + counter++;
					}

					@Provides
					@Singleton
					Integer provide(@Named("first") WorkerPool workerPool1, @Named("second") WorkerPool workerPool2) {
						workerPool1.getInstances(String.class);
						workerPool2.getInstances(String.class);
						return 0;
					}
				},
				TriggersModule.create()
						.with(String.class, Severity.HIGH, "test", s -> TriggerResult.create())
		);
		injector.getInstance(Key.get(WorkerPool.class, named("first"))).getInstances(String.class);
		injector.getInstance(Key.get(WorkerPool.class, named("second"))).getInstances(String.class);
		ServiceGraph serviceGraph = injector.getInstance(ServiceGraph.class);
		serviceGraph.startFuture().get();
		Triggers triggersWatcher = injector.getInstance(Triggers.class);
		assertEquals(firstPoolSize + secondPoolSize, triggersWatcher.getResults().size());
		triggersWatcher.getResults()
				.forEach(triggerWithResult -> assertTrue(triggerWithResult.toString().startsWith("HIGH : String : test :: ")));
	}

}
