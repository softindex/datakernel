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
import io.datakernel.service.Service;
import io.datakernel.service.ServiceGraph;
import io.datakernel.service.ServiceGraphModule;
import org.junit.Test;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.Assert.assertEquals;

public class TestStartTwice {
	private static AtomicInteger countStart = new AtomicInteger(0);
	private static AtomicInteger countStop = new AtomicInteger(0);

	interface A extends Service {}

	static class ServiceImpl implements A {
		@Override
		public CompletableFuture<Void> start() {
			countStart.incrementAndGet();
			return CompletableFuture.completedFuture(null);
		}

		@Override
		public CompletableFuture<Void> stop() {
			countStop.incrementAndGet();
			return CompletableFuture.completedFuture(null);
		}
	}

	static class TestModule extends AbstractModule {

		@Override
		protected void configure() {
			install(ServiceGraphModule.defaultInstance());
		}

		@Provides
		@Singleton
		ServiceImpl serviceImpl(A a) {
			return (ServiceImpl) a;
		}

		@Provides
		@Singleton
		A createA() {
			return new ServiceImpl();
		}

	}

	@Test
	public void test() throws Exception {
		Injector injector = Guice.createInjector(Stage.PRODUCTION, new TestModule());
		ServiceGraph serviceGraph = injector.getInstance(ServiceGraph.class);

		try {
			serviceGraph.startFuture().get();
		} finally {
			serviceGraph.stopFuture().get();
		}

		assertEquals(countStart.get(), 1);
		assertEquals(countStop.get(), 1);
	}
}
