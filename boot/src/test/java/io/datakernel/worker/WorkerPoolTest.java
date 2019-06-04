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
import io.datakernel.eventloop.FatalErrorHandlers;
import org.junit.Before;
import org.junit.Test;

import java.util.concurrent.ExecutionException;

import static org.junit.Assert.assertEquals;

public class WorkerPoolTest {
	private WorkerPool first;
	private WorkerPool second;
	private WorkerPool.Instances<Eventloop> eventloopsFirst;
	private WorkerPool.Instances<Eventloop> eventloopsSecond;

	@Before
	public void setUp() {
		TestModule.counter = 0;
		Injector injector = Injector.of(new TestModule(), new WorkerPoolModule());
		first = injector.getInstance(Key.of(WorkerPool.class, "First"));
		second = injector.getInstance(Key.of(WorkerPool.class, "Second"));
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
//		Provider<String> stringProviderFirst = first.getCurrentInstanceProvider(String.class);
//		Provider<String> stringProviderSecond = second.getCurrentInstanceProvider(String.class);
//		for (Integer i = 0; i < eventloopsFirst.size() + eventloopsSecond.size(); i++) {
//			Integer count = i;
//			Eventloop eventloop = i < eventloopsFirst.size() ? eventloopsFirst.get(i) : eventloopsSecond.get(i - eventloopsFirst.size());
//			Future<String> result = Executors.newSingleThreadExecutor().submit(
//					() -> {
//						CompletableFuture<String> submit = eventloop.submit(() -> {
//							if (count < eventloopsFirst.size()) {
//								return stringProviderFirst.get();
//							}
//							return stringProviderSecond.get();
//						});
//						eventloop.run();
//						return submit.get();
//					});
//			assertTrue(result.get().endsWith(String.valueOf(i)));
//		}
	}

	@Test
	public void testGetCurrentInstance() throws ExecutionException, InterruptedException {
//		for (Integer i = 0; i < eventloopsFirst.size() + eventloopsSecond.size(); i++) {
//			Integer count = i;
//			Eventloop eventloop = i < eventloopsFirst.size() ? eventloopsFirst.get(i) : eventloopsSecond.get(i - eventloopsFirst.size());
//			Future<String> result = Executors.newSingleThreadExecutor().submit(
//					() -> {
//						CompletableFuture<String> submit = eventloop.submit(() -> {
//							if (count < eventloopsFirst.size()) {
//								return first.getCurrentInstance(String.class);
//							}
//							return second.getCurrentInstance(String.class);
//						});
//						eventloop.run();
//						return submit.get();
//					});
//			assertTrue(result.get().endsWith(String.valueOf(i)));
//		}
	}

	@Test
	public void testGetCurrentInstanceWithoutEventloop() {
//		AtomicBoolean wasExecuted = new AtomicBoolean(false);
//		ExecutorService executorService = Executors.newSingleThreadExecutor();
//		Future<String> future = executorService.submit(() -> first.getCurrentInstance(String.class));
//		try {
//			future.get();
//		} catch (Throwable e) {
//			e = e.getCause();
//			wasExecuted.set(true);
//			assertEquals(IllegalStateException.class, e.getClass());
//			assertTrue(e.getMessage().contains("Trying to start async operations prior eventloop.run()"));
//		}
//		assertTrue(wasExecuted.get());
	}

	@Test
	public void testGetCurrentInstanceFromUnknownThread() {
//		AtomicBoolean wasExecuted = new AtomicBoolean(false);
//		ExecutorService executorService = Executors.newSingleThreadExecutor();
//		Future<String> future = executorService.submit(() -> {
//			Eventloop eventloop = Eventloop.create().withCurrentThread().withFatalErrorHandler(FatalErrorHandlers.rethrowOnAnyError());
//			CompletableFuture<String> submit = eventloop.submit(() -> first.getCurrentInstance(String.class));
//			eventloop.run();
//			return submit.get();
//		});
//		try {
//			future.get();
//		} catch (Throwable e) {
//			wasExecuted.set(true);
//			e = e.getCause();
//			assertEquals(IllegalStateException.class, e.getClass());
//			assertEquals("No instance of Key[type=java.lang.String, annotation=[none]] is associated with current thread", e.getMessage());
//		}
//		assertTrue(wasExecuted.get());
	}

	static class TestModule extends AbstractModule {
		static int counter = 0;

		@Provides
		@Worker
		String string() {
			return "String number " + counter++;
		}

		@Provides
		@Worker
		Eventloop workerEventloop() {
			return Eventloop.create().withFatalErrorHandler(FatalErrorHandlers.rethrowOnAnyError());
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

