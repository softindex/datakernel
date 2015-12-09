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
import io.datakernel.bytebuf.ByteBufPool;
import io.datakernel.guice.servicegraph.AsyncServiceAdapter;
import io.datakernel.guice.servicegraph.ServiceGraphModule;
import io.datakernel.guice.workers.NioWorkerModule;
import io.datakernel.guice.workers.NioWorkerScopeFactory;
import io.datakernel.guice.workers.WorkerId;
import io.datakernel.guice.workers.WorkerThread;
import io.datakernel.service.AsyncService;
import io.datakernel.service.AsyncServices;
import io.datakernel.service.ServiceGraph;
import org.junit.Before;
import org.junit.Test;

import java.util.List;
import java.util.concurrent.Executor;

import static io.datakernel.bytebuf.ByteBufPool.getPoolItemsString;
import static org.junit.Assert.assertEquals;

public class WorkerThreadNameTest {
	public static final int PORT = 7583;
	public static final int WORKERS = 4;

	@Before
	public void before() {
		ByteBufPool.clear();
		ByteBufPool.setSizes(0, Integer.MAX_VALUE);
	}

	public static class Element1 {}

	public static class Element2 {}

	public static class Element3 {}

	public static class Element4 {}

	public static class TestModule extends AbstractModule {
		@Override
		protected void configure() {
			install(new NioWorkerModule());
			install(new ServiceGraphModule()
					.register(Element4.class, new AsyncServiceAdapter<Element4>() {
						@Override
						public AsyncService toService(Element4 node, Executor executor) {
							return AsyncServices.immediateService();
						}
					})
					.register(Element1.class, new AsyncServiceAdapter<Element1>() {
						@Override
						public AsyncService toService(Element1 node, Executor executor) {
							return AsyncServices.immediateService();
						}
					})
					.register(Element2.class, new AsyncServiceAdapter<Element2>() {
						@Override
						public AsyncService toService(Element2 node, Executor executor) {
							return AsyncServices.immediateService();
						}
					})
					.register(Element3.class, new AsyncServiceAdapter<Element3>() {
						@Override
						public AsyncService toService(Element3 node, Executor executor) {
							return AsyncServices.immediateService();
						}
					}));
		}

		@Provides
		@Singleton
		Element1 primaryEventloop() {
			return new Element1();
		}

		@Provides
		@Singleton
		Element2 primaryNioServer(Element1 primaryEventloop,
		                                  NioWorkerScopeFactory nioWorkerScope,
		                                  @WorkerThread("First") Provider<Element4> unusedStringProvider,
		                                  @WorkerThread Provider<Element3> itemProvider) {
			List<Element4> unusedList = nioWorkerScope.getList(WORKERS, unusedStringProvider);
			List<Element3> workerHttpServers = nioWorkerScope.getList(WORKERS, itemProvider);
			return new Element2();
		}

		@Provides
		@WorkerThread("First")
		Element4 ffWorker() {
			return new Element4();
		}

		@Provides
		@WorkerThread("Second")
		Element4 fSWorker() {
			return new Element4();
		}

		@Provides
		@WorkerThread
		Element1 workerEventloop() {
			return new Element1();
		}

		@Provides
		@WorkerThread
		Element3 workerHttpServer(@WorkerThread Element1 eventloop, @WorkerId final int workerId,
		                                 @WorkerThread("Second") Element4 unusedString) {
			return new Element3();
		}

	}

	@Test
	public void test() throws Exception {
		Injector injector = Guice.createInjector(Stage.PRODUCTION, new TestModule());
		ServiceGraph serviceGraph = ServiceGraphModule.getServiceGraph(injector, Key.get(Element2.class));
		try {
			serviceGraph.start();
		} finally {
			serviceGraph.stop();
		}

		assertEquals(getPoolItemsString(), ByteBufPool.getCreatedItems(), ByteBufPool.getPoolItems());
	}
}