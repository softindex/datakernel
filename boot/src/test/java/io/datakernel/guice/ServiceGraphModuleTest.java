/*
 * Copyright (C) 2015 SoftIndex LLC.
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

package io.datakernel.guice;

import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.Key;
import io.datakernel.guice.servicegraph.AsyncServiceAdapter;
import io.datakernel.guice.servicegraph.ServiceGraphModule;
import io.datakernel.service.AsyncService;
import io.datakernel.service.AsyncServiceCallbacks;
import io.datakernel.service.AsyncServices;
import io.datakernel.service.ServiceGraph;
import org.junit.Test;

import java.util.concurrent.Executor;

public class ServiceGraphModuleTest {

	@Test
	public void testStartStop() throws Exception {
		Injector injector = Guice.createInjector(
				new ServiceGraphModule()
						.register(TestGraph.S.class, new AsyncServiceAdapter<TestGraph.S>() {
							@Override
							public AsyncService toService(TestGraph.S service, Executor executor) {
								return AsyncServices.immediateService();
							}
						})
						.addDependency(Key.get(TestGraph.S6.class), Key.get(TestGraph.S4.class))
						.removeDependency(Key.get(TestGraph.S4.class), Key.get(TestGraph.S2.class))
						.addDependency(Key.get(TestGraph.S6.class), Key.get(TestGraph.S3.class)) // Duplicate existing dependency
						.removeDependency(Key.get(TestGraph.S4.class), Key.get(TestGraph.S6.class)) // Removing non-existing dependency
				, new TestGraph.Module());
		ServiceGraph serviceGraph = injector.getInstance(ServiceGraph.class);
		injector.getInstance(TestGraph.O5.class);
		// TODO (nkhohlov): remove s1..s6? (Warning The value of the local variable s1 is not used)
		TestGraph.S1 s1 = injector.getInstance(TestGraph.S1.class);
		TestGraph.S2 s2 = injector.getInstance(TestGraph.S2.class);
		TestGraph.S3 s3 = injector.getInstance(TestGraph.S3.class);
		TestGraph.S4 s4 = injector.getInstance(TestGraph.S4.class);
		TestGraph.S5 s5 = injector.getInstance(TestGraph.S5.class);
		TestGraph.S6 s6 = injector.getInstance(TestGraph.S6.class);

		AsyncServiceCallbacks.BlockingServiceCallback startCallback = AsyncServiceCallbacks.withCountDownLatch();
		serviceGraph.start(startCallback);
		startCallback.await();

		AsyncServiceCallbacks.BlockingServiceCallback stopCallback = AsyncServiceCallbacks.withCountDownLatch();
		serviceGraph.stop(stopCallback);
		stopCallback.await();
	}

	@Test
	public void testStartStopWithoutOverride() throws Exception {
		Injector injector = Guice.createInjector(
				new ServiceGraphModule()
						.register(TestGraph.S.class, new AsyncServiceAdapter<TestGraph.S>() {
							@Override
							public AsyncService toService(TestGraph.S service, Executor executor) {
								return AsyncServices.immediateService();
							}
						})
				, new TestGraph.Module());
		ServiceGraph serviceGraph = injector.getInstance(ServiceGraph.class);
		injector.getInstance(TestGraph.O5.class);
		TestGraph.S1 s1 = injector.getInstance(TestGraph.S1.class);
		TestGraph.S2 s2 = injector.getInstance(TestGraph.S2.class);
		TestGraph.S3 s3 = injector.getInstance(TestGraph.S3.class);
		TestGraph.S4 s4 = injector.getInstance(TestGraph.S4.class);
		TestGraph.S5 s5 = injector.getInstance(TestGraph.S5.class);
		TestGraph.S6 s6 = injector.getInstance(TestGraph.S6.class);

		AsyncServiceCallbacks.BlockingServiceCallback startCallback = AsyncServiceCallbacks.withCountDownLatch();
		serviceGraph.start(startCallback);
		startCallback.await();

		AsyncServiceCallbacks.BlockingServiceCallback stopCallback = AsyncServiceCallbacks.withCountDownLatch();
		serviceGraph.stop(stopCallback);
		stopCallback.await();
	}

}