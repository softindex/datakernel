package io.datakernel.worker;

import com.google.inject.*;
import com.google.inject.name.Names;
import io.datakernel.eventloop.Eventloop;
import io.datakernel.eventloop.FatalErrorHandlers;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import javax.inject.Named;
import javax.inject.Singleton;
import java.util.HashSet;
import java.util.List;
import java.util.concurrent.*;

import static io.datakernel.eventloop.FatalErrorHandlers.rethrowOnAnyError;
import static io.datakernel.util.CollectionUtils.set;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class WorkerPoolsTest {
	@Rule
	public ExpectedException expectedException = ExpectedException.none();

	private Injector injector;
	private WorkerPool first;
	private WorkerPool second;
	private WorkerPools pools;
	private List<Eventloop> eventloopsFirst;
	private List<Eventloop> eventloopsSecond;

	@Before
	public void setUp() {
		TestModule.counter = 0;
		injector = Guice.createInjector(new TestModule(), new WorkerPoolModule());
		first = injector.getInstance(Key.get(WorkerPool.class, Names.named("First")));
		second = injector.getInstance(Key.get(WorkerPool.class, Names.named("Second")));
		eventloopsFirst = first.getInstances(Eventloop.class);
		eventloopsSecond = second.getInstances(Eventloop.class);
		pools = injector.getInstance(WorkerPools.class);
	}

	@Test
	public void testAddWorkerPool() {
		WorkerPools workerPools = injector.getInstance(WorkerPools.class);
		assertEquals(set(first, second), new HashSet<>(workerPools.getWorkerPools()));
	}

	@Test
	public void testAddWorkerPoolWithDuplicates() {
		WorkerPools workerPools = new WorkerPools();
		workerPools.addWorkerPool(first);
		expectedException.expect(IllegalArgumentException.class);
		expectedException.expectMessage("WorkerPool has already been added");
		workerPools.addWorkerPool(first);
	}

	@Test
	public void testGetCurrentWorkerPool() throws ExecutionException, InterruptedException {
		Eventloop eventloop = first.getInstances(Eventloop.class).get(0);
		CompletableFuture<WorkerPool> submit = eventloop.submit(() -> injector.getInstance(WorkerPools.class).getCurrentWorkerPool());
		eventloop.run();
		assertEquals(first, submit.get());
	}

	@Test
	public void testGetCurrentWorkerPoolWithoutEventloop() throws Throwable {
		ExecutorService executorService = Executors.newSingleThreadExecutor();
		Future<WorkerPool> future = executorService.submit(() -> injector.getInstance(WorkerPools.class).getCurrentWorkerPool());
		try {
			future.get();
			Assert.fail("It should fail");
		} catch (Exception e) {
			Throwable cause = e.getCause();
			expectedException.expect(IllegalStateException.class);
			expectedException.expectMessage("Trying to start async operations prior eventloop.run()");
			throw cause;
		}
	}

	@Test
	public void testGetCurrentWorkerPoolFromUnknownThread() throws Throwable {
		ExecutorService executorService = Executors.newSingleThreadExecutor();
		Future<WorkerPool> future = executorService.submit(() -> {
			Eventloop eventloop = Eventloop.create().withFatalErrorHandler(rethrowOnAnyError());
			CompletableFuture<WorkerPool> submit = eventloop.submit(() -> {
				first.getInstances(Eventloop.class); // trigger all eventloops to be created and put in the pools
				second.getInstances(Eventloop.class);
				return injector.getInstance(WorkerPools.class).getCurrentWorkerPool();
			});
			eventloop.run();
			return submit.get();
		});
		try {
			future.get();
			Assert.fail("It should fail");
		} catch (Exception e) {
			Throwable cause = e.getCause().getCause();
			expectedException.expect(IllegalStateException.class);
			expectedException.expectMessage("No WorkerPool is associated with current thread");
			throw cause;
		}
	}

	@Test
	public void testGetCurrentInstanceProvider() throws ExecutionException, InterruptedException {
		Provider<String> provider = pools.getCurrentInstanceProvider(Key.get(String.class));

		// calling 'get' outside of worker's thread
		ExecutorService executorService = Executors.newSingleThreadExecutor();
		Future<String> future = executorService.submit(() -> {
			Eventloop eventloop = Eventloop.create().withCurrentThread().withFatalErrorHandler(FatalErrorHandlers.rethrowOnAnyError());
			CompletableFuture<String> submit = eventloop.submit(provider::get);
			eventloop.run();
			return submit.get();
		});
		try {
			future.get();
		} catch (Throwable e) {
			e = e.getCause().getCause();
			assertEquals(IllegalStateException.class, e.getClass());
			assertEquals("No WorkerPool is associated with current thread", e.getMessage());
		}

		// calling 'get' inside worker threads
		for (Integer i = 0; i < eventloopsFirst.size() + eventloopsSecond.size(); i++) {
			Integer count = i;
			Eventloop eventloop = i < eventloopsFirst.size() ? eventloopsFirst.get(i) : eventloopsSecond.get(i - eventloopsFirst.size());
			Future<String> result = Executors.newSingleThreadExecutor().submit(
					() -> {
						CompletableFuture<String> submit = eventloop.submit(() -> {
							if (count < eventloopsFirst.size()) {
								return provider.get();
							}
							return provider.get();
						});
						eventloop.run();
						return submit.get();
					});
			assertTrue(result.get().endsWith(String.valueOf(i)));
		}
	}

	static class TestModule extends AbstractModule {
		static int counter;

		@Provides
		@Worker
		Eventloop provideWorkerEventloop() {
			return Eventloop.create().withFatalErrorHandler(rethrowOnAnyError());
		}

		@Provides
		@Worker
		String provideString() {
			return "String: " + counter++;
		}

		@Provides
		@Singleton
		@Named("First")
		WorkerPool provideFirstWorkerPool() {
			return new WorkerPool(4);
		}

		@Provides
		@Singleton
		@Named("Second")
		WorkerPool provideSecondWorkerPool() {
			return new WorkerPool(10);
		}
	}
}
