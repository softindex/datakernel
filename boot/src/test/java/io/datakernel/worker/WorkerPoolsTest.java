/*
 * Copyright (C) 2015-2018 SoftIndex LLC.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.datakernel.worker;

import io.datakernel.di.Injector;
import io.datakernel.di.Key;
import io.datakernel.di.Named;
import io.datakernel.di.module.AbstractModule;
import io.datakernel.di.module.Provides;
import io.datakernel.eventloop.Eventloop;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.util.HashSet;
import java.util.concurrent.ExecutionException;

import static io.datakernel.eventloop.FatalErrorHandlers.rethrowOnAnyError;
import static io.datakernel.util.CollectionUtils.set;
import static org.junit.Assert.assertEquals;

public class WorkerPoolsTest {
	@Rule
	public ExpectedException expectedException = ExpectedException.none();

	private Injector injector;
	private WorkerPool first;
	private WorkerPool second;
	private WorkerPools pools;
	private WorkerPool.Instances<Eventloop> eventloopsFirst;
	private WorkerPool.Instances<Eventloop> eventloopsSecond;

	@Before
	public void setUp() {
		TestModule.counter = 0;
		injector = Injector.of(new TestModule(), new WorkerPoolModule());
		first = injector.getInstance(Key.of(WorkerPool.class, "First"));
		second = injector.getInstance(Key.of(WorkerPool.class, "Second"));
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
//		WorkerPools workerPools = new WorkerPools(injector);
//		workerPools.addWorkerPool(first);
//		expectedException.expect(IllegalArgumentException.class);
//		expectedException.expectMessage("WorkerPool has already been added");
//		workerPools.addWorkerPool(first);
	}

	@Test
	public void testGetCurrentWorkerPool() throws ExecutionException, InterruptedException {
//		Eventloop eventloop = first.getInstances(Eventloop.class).get(0);
//		CompletableFuture<WorkerPool> submit = eventloop.submit(() -> injector.getInstance(WorkerPools.class).getCurrentWorkerPool());
//		eventloop.run();
//		assertEquals(first, submit.get());
	}

	@Test
	public void testGetCurrentWorkerPoolWithoutEventloop() throws Throwable {
//		ExecutorService executorService = Executors.newSingleThreadExecutor();
//		Future<WorkerPool> future = executorService.submit(() -> injector.getInstance(WorkerPools.class).getCurrentWorkerPool());
//		try {
//			future.get();
//			Assert.fail("It should fail");
//		} catch (Exception e) {
//			Throwable cause = e.getCause();
//			expectedException.expect(IllegalStateException.class);
//			expectedException.expectMessage("Trying to start async operations prior eventloop.run()");
//			//noinspection ThrowInsideCatchBlockWhichIgnoresCaughtException - cause is rethrown
//			throw cause;
//		}
	}

	@Test
	public void testGetCurrentWorkerPoolFromUnknownThread() throws Throwable {
//		ExecutorService executorService = Executors.newSingleThreadExecutor();
//		Future<WorkerPool> future = executorService.submit(() -> {
//			Eventloop eventloop = Eventloop.create().withFatalErrorHandler(rethrowOnAnyError());
//			CompletableFuture<WorkerPool> submit = eventloop.submit(() -> {
//				first.getInstances(Eventloop.class); // trigger all eventloops to be created and put in the pools
//				second.getInstances(Eventloop.class);
//				return injector.getInstance(WorkerPools.class).getCurrentWorkerPool();
//			});
//			eventloop.run();
//			return submit.get();
//		});
//		try {
//			future.get();
//			Assert.fail("It should fail");
//		} catch (Exception e) {
//			Throwable cause = e.getCause();
//			expectedException.expect(IllegalStateException.class);
//			expectedException.expectMessage("No WorkerPool is associated with current thread");
//			//noinspection ThrowInsideCatchBlockWhichIgnoresCaughtException - cause is rethrown
//			throw cause;
//		}
	}

	@Test
	public void testGetCurrentInstanceProvider() throws ExecutionException, InterruptedException {
//		Provider<String> provider = pools.getCurrentInstanceProvider(Key.get(String.class));
//
//		// calling 'get' outside of worker's thread
//		ExecutorService executorService = Executors.newSingleThreadExecutor();
//		Future<String> future = executorService.submit(() -> {
//			Eventloop eventloop = Eventloop.create().withCurrentThread().withFatalErrorHandler(FatalErrorHandlers.rethrowOnAnyError());
//			CompletableFuture<String> submit = eventloop.submit(provider::get);
//			eventloop.run();
//			return submit.get();
//		});
//		try {
//			future.get();
//		} catch (Throwable e) {
//			e = e.getCause();
//			assertEquals(IllegalStateException.class, e.getClass());
//			assertEquals("No WorkerPool is associated with current thread", e.getMessage());
//		}
//
//		// calling 'get' inside worker threads
//		for (Integer i = 0; i < eventloopsFirst.size() + eventloopsSecond.size(); i++) {
//			Integer count = i;
//			Eventloop eventloop = i < eventloopsFirst.size() ? eventloopsFirst.get(i) : eventloopsSecond.get(i - eventloopsFirst.size());
//			Future<String> result = Executors.newSingleThreadExecutor().submit(
//					() -> {
//						CompletableFuture<String> submit = eventloop.submit(() -> {
//							if (count < eventloopsFirst.size()) {
//								return provider.get();
//							}
//							return provider.get();
//						});
//						eventloop.run();
//						return submit.get();
//					});
//			assertTrue(result.get().endsWith(String.valueOf(i)));
//		}
	}

	static class TestModule extends AbstractModule {
		static int counter;

		@Provides
		@Worker
		Eventloop workerEventloop() {
			return Eventloop.create().withFatalErrorHandler(rethrowOnAnyError());
		}

		@Provides
		@Worker
		String string() {
			return "String: " + counter++;
		}

		@Provides
		@Named("First")
		WorkerPool firstWorkerPool(WorkerPools pools) {
			return pools.createPool(4);
		}

		@Provides
		@Named("Second")
		WorkerPool secondWorkerPool(WorkerPools pools) {
			return pools.createPool(10);
		}
	}
}
