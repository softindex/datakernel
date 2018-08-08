package io.datakernel.worker;

import com.google.inject.*;
import com.google.inject.name.Names;
import io.datakernel.eventloop.Eventloop;
import io.datakernel.eventloop.FatalErrorHandlers;
import org.junit.Before;
import org.junit.Test;

import javax.inject.Named;
import javax.inject.Singleton;
import java.util.List;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class WorkerPoolTest {
	private WorkerPool first;
	private WorkerPool second;
	private List<Eventloop> eventloopsFirst;
	private List<Eventloop> eventloopsSecond;

	@Before
	public void setUp() {
		TestModule.counter = 0;
		Injector injector = Guice.createInjector(new TestModule(), new WorkerPoolModule());
		first = injector.getInstance(Key.get(WorkerPool.class, Names.named("First")));
		second = injector.getInstance(Key.get(WorkerPool.class, Names.named("Second")));
		eventloopsFirst = first.getInstances(Eventloop.class);
		eventloopsSecond = second.getInstances(Eventloop.class);
	}

	@Test
	public void testInjector() {
		assertEquals(4, first.getInstances(Eventloop.class).size());
		assertEquals(10, second.getInstances(Eventloop.class).size());
	}

	@Test
	public void testProvider() throws ExecutionException, InterruptedException {
		Provider<String> stringProviderFirst = first.getCurrentInstanceProvider(String.class);
		Provider<String> stringProviderSecond = second.getCurrentInstanceProvider(String.class);
		for (Integer i = 0; i < eventloopsFirst.size() + eventloopsSecond.size(); i++) {
			Integer count = i;
			Eventloop eventloop = i < eventloopsFirst.size() ? eventloopsFirst.get(i) : eventloopsSecond.get(i - eventloopsFirst.size());
			Future<String> result = Executors.newSingleThreadExecutor().submit(
					() -> {
						CompletableFuture<String> submit = eventloop.submit(() -> {
							if (count < eventloopsFirst.size()) {
								return stringProviderFirst.get();
							}
							return stringProviderSecond.get();
						});
						eventloop.run();
						return submit.get();
					});
			assertTrue(result.get().endsWith(String.valueOf(i)));
		}
	}

	@Test
	public void testGetCurrentInstance() throws ExecutionException, InterruptedException {
		for (Integer i = 0; i < eventloopsFirst.size() + eventloopsSecond.size(); i++) {
			Integer count = i;
			Eventloop eventloop = i < eventloopsFirst.size() ? eventloopsFirst.get(i) : eventloopsSecond.get(i - eventloopsFirst.size());
			Future<String> result = Executors.newSingleThreadExecutor().submit(
					() -> {
						CompletableFuture<String> submit = eventloop.submit(() -> {
							if (count < eventloopsFirst.size()) {
								return first.getCurrentInstance(String.class);
							}
							return second.getCurrentInstance(String.class);
						});
						eventloop.run();
						return submit.get();
					});
			assertTrue(result.get().endsWith(String.valueOf(i)));
		}
	}

	@Test
	public void testGetCurrentInstanceWithoutEventloop() {
		AtomicBoolean wasExecuted = new AtomicBoolean(false);
		ExecutorService executorService = Executors.newSingleThreadExecutor();
		Future<String> future = executorService.submit(() -> first.getCurrentInstance(String.class));
		try {
			future.get();
		} catch (Throwable e) {
			e = e.getCause();
			wasExecuted.set(true);
			assertEquals(IllegalStateException.class, e.getClass());
			assertTrue(e.getMessage().contains("Trying to start async operations prior eventloop.run()"));
		}
		assertTrue(wasExecuted.get());
	}

	@Test
	public void testGetCurrentInstanceFromUnknownThread() {
		AtomicBoolean wasExecuted = new AtomicBoolean(false);
		ExecutorService executorService = Executors.newSingleThreadExecutor();
		Future<String> future = executorService.submit(() -> {
			Eventloop eventloop = Eventloop.create().withCurrentThread().withFatalErrorHandler(FatalErrorHandlers.rethrowOnAnyError());
			CompletableFuture<String> submit = eventloop.submit(() -> first.getCurrentInstance(String.class));
			eventloop.run();
			return submit.get();
		});
		try {
			future.get();
		} catch (Throwable e) {
			wasExecuted.set(true);
			e = e.getCause().getCause();
			assertEquals(IllegalStateException.class, e.getClass());
			assertEquals("No instance of Key[type=java.lang.String, annotation=[none]] is associated with current thread", e.getMessage());
		}
		assertTrue(wasExecuted.get());
	}

	static class TestModule extends AbstractModule {
		static int counter = 0;

		@Provides
		@Worker
		String provideString() {
			return "String number " + counter++;
		}

		@Provides
		@Worker
		Eventloop provideWorkerEventloop() {
			return Eventloop.create().withFatalErrorHandler(FatalErrorHandlers.rethrowOnAnyError());
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

