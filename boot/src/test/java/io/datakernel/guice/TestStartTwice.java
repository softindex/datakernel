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

import com.google.inject.*;
import io.datakernel.guice.servicegraph.AsyncServiceAdapters;
import io.datakernel.guice.servicegraph.ServiceGraphModule;
import io.datakernel.service.AsyncService;
import io.datakernel.service.AsyncServiceCallback;
import io.datakernel.service.ServiceGraph;
import org.junit.Test;

import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.Assert.assertEquals;

public class TestStartTwice {
	private static AtomicInteger countStart = new AtomicInteger(0);
	private static AtomicInteger countStop = new AtomicInteger(0);

	interface A extends AsyncService {}

	interface B extends AsyncService {}

	interface C extends AsyncService {}

	static class ServiceImpl implements A, B, C {

		@Override
		public void start(AsyncServiceCallback callback) {
			countStart.incrementAndGet();
			callback.onComplete();
		}

		@Override
		public void stop(AsyncServiceCallback callback) {
			countStop.incrementAndGet();
			callback.onComplete();
		}
	}

	private static ServiceImpl serviceImpl = new ServiceImpl();

	static class TestModule extends AbstractModule {

		@Override
		protected void configure() {
			install(new ServiceGraphModule()
					.register(ServiceImpl.class, AsyncServiceAdapters.forAsyncService())
					.register(A.class, AsyncServiceAdapters.forAsyncService())
					.register(B.class, AsyncServiceAdapters.forAsyncService())
					.register(C.class, AsyncServiceAdapters.forAsyncService()));
		}

		@Provides
		@Singleton
		ServiceImpl serviceImpl(A a, B b, C c) {
			return serviceImpl;
		}

		@Provides
		@Singleton
		A createA(B b, C c) {
			return serviceImpl;
		}

		@Provides
		@Singleton
		B createB(C c) {
			return serviceImpl;
		}

		@Provides
		@Singleton
		C createC() {
			return serviceImpl;
		}
	}

	@Test
	public void test() throws Exception {
		Injector injector = Guice.createInjector(new TestModule());
		ServiceGraph serviceGraph = ServiceGraphModule.getServiceGraph(injector, ServiceImpl.class);

		try {
			serviceGraph.start();
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			serviceGraph.stop();
		}

		assertEquals(countStart.get(), 1);
		assertEquals(countStop.get(), 1);
	}
}
