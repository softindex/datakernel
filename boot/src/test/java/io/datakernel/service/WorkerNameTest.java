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

package io.datakernel.service;

import io.datakernel.di.Injector;
import io.datakernel.di.Key;
import io.datakernel.di.Named;
import io.datakernel.di.module.AbstractModule;
import io.datakernel.di.module.Provides;
import io.datakernel.test.rules.ByteBufRule;
import io.datakernel.worker.Worker;
import io.datakernel.worker.WorkerId;
import io.datakernel.worker.WorkerPool;
import io.datakernel.worker.WorkerPools;
import org.junit.Rule;
import org.junit.Test;

import static io.datakernel.service.ServiceAdapters.combinedAdapter;
import static io.datakernel.service.ServiceAdapters.immediateServiceAdapter;

public final class WorkerNameTest {
	public static final int WORKERS = 4;

	@Rule
	public ByteBufRule byteBufRule = new ByteBufRule();

	public static class Element1 {}

	public static class Element2 {}

	public static class Element3 {}

	public static class Element4 extends Element1 {}

	public static class TestModule extends AbstractModule {
		@Override
		protected void configure() {
			install(ServiceGraphModule.defaultInstance()
					.register(Element4.class, combinedAdapter(
							immediateServiceAdapter(),
							immediateServiceAdapter()))
					.register(Element1.class, immediateServiceAdapter())
					.register(Element2.class, immediateServiceAdapter())
					.register(Element3.class, immediateServiceAdapter()));
		}

		@Provides
		@Named("Primary")
		Element1 primaryEventloop() {
			return new Element1();
		}

		@Provides
		Element2 primaryServer(@Named("Primary") Element1 primaryEventloop, WorkerPool workerPool) {
			WorkerPool.Instances<Element4> unusedList = workerPool.getInstances(Key.of(Element4.class, "First"));
			return new Element2();
		}

		@Provides
		WorkerPool workerPool(WorkerPools workerPools) {
			return workerPools.createPool(WORKERS);
		}

		@Provides
		@Worker
		@Named("First")
		Element4 ffWorker() {
			return new Element4();
		}

		@Provides
		@Worker
		@Named("Second")
		Element4 fSWorker() {
			return new Element4();
		}

		@Provides
		@Worker
		Element1 workerEventloop() {
			return new Element1();
		}

		@Provides
		@Worker
		Element3 workerHttpServer(Element1 eventloop, @WorkerId int workerId,
		                          @Named("Second") Element4 unusedString) {
			return new Element3();
		}

	}

	@Test
	public void test() throws Exception {
		Injector injector = Injector.of(new TestModule());
		injector.getInstance(Element2.class);
		ServiceGraph serviceGraph = injector.getInstance(ServiceGraph.class);
		try {
			serviceGraph.startFuture().get();
		} finally {
			serviceGraph.stopFuture().get();
		}
	}
}
