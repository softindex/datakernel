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

package io.datakernel.guice;

import com.google.inject.*;
import com.google.inject.name.Named;
import io.datakernel.service.ServiceGraph;
import io.datakernel.service.ServiceGraphModule;
import io.datakernel.test.rules.ByteBufRule;
import io.datakernel.worker.Worker;
import org.junit.Rule;
import org.junit.Test;

import java.util.List;

import static com.google.inject.name.Names.named;
import static io.datakernel.service.ServiceAdapters.combinedAdapter;
import static io.datakernel.service.ServiceAdapters.immediateServiceAdapter;
import static io.datakernel.test.TestUtils.getFreePort;

public final class WorkerNameTest {
	public static final int PORT = getFreePort();
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
		@Singleton
		WorkerPool workerPool() {
			return new WorkerPool(WORKERS);
		}

		@Provides
		@Singleton
		@Named("Primary")
		Element1 primaryEventloop() {
			return new Element1();
		}

		@Provides
		@Singleton
		Element2 primaryServer(@Named("Primary") Element1 primaryEventloop, WorkerPool workerPool) {
			List<Element4> unusedList = workerPool.getInstances(Key.get(Element4.class, named("First")));
			return new Element2();
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
		Element3 workerHttpServer(Element1 eventloop, int workerId,
		                          @Named("Second") Element4 unusedString) {
			return new Element3();
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
	}
}
