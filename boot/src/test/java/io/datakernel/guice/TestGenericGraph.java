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
import io.datakernel.guice.servicegraph.AsyncServiceAdapter;
import io.datakernel.guice.servicegraph.ServiceGraphModule;
import io.datakernel.guice.workers.NioWorkerModule;
import io.datakernel.guice.workers.NioWorkerScopeFactory;
import io.datakernel.guice.workers.WorkerThread;
import io.datakernel.service.AsyncService;
import io.datakernel.service.AsyncServices;
import io.datakernel.service.ServiceGraph;
import org.junit.Test;

import java.util.List;
import java.util.concurrent.Executor;

import static org.junit.Assert.assertEquals;

public class TestGenericGraph {
	public static final int WORKERS = 1;

	public static class Pojo<T> {
		private final T object;

		public Pojo(T object) {this.object = object;}

		public T getObject() {
			return object;
		}
	}

	public static class TestModule extends AbstractModule {

		@Override
		protected void configure() {
			install(new NioWorkerModule());
			install(new ServiceGraphModule()
					.register(Pojo.class, new AsyncServiceAdapter<Pojo>() {
						@Override
						public AsyncService toService(Pojo node, Executor executor) {
							return AsyncServices.immediateService();
						}
					}));
		}

		@Provides
		@Singleton
		Pojo<Integer> integerPojo(NioWorkerScopeFactory nioWorkerScope,
		                          @WorkerThread Provider<Pojo<String>> pojoProvider,
		                          @WorkerThread("other") Provider<Pojo<String>> pojoProviderOther) {
			List<Pojo<String>> list = nioWorkerScope.getList(WORKERS, pojoProvider);
			List<Pojo<String>> listOther = nioWorkerScope.getList(WORKERS, pojoProviderOther);
			return new Pojo<>(Integer.valueOf(listOther.get(0).getObject())
					+ Integer.valueOf(list.get(0).getObject()));
		}

		@Provides
		@WorkerThread
		Pojo<String> stringPojo(@WorkerThread("other") Pojo<String> stringPojo) {
			return new Pojo<>("123");
		}

		@Provides
		@WorkerThread("other")
		Pojo<String> stringPojoOther() {
			return new Pojo<>("456");
		}
	}

	@Test
	public void test() throws Exception {
		Injector injector = Guice.createInjector(new TestModule());
		ServiceGraph serviceGraph =
				ServiceGraphModule.getServiceGraph(injector, Key.get(new TypeLiteral<Pojo<Integer>>() {}));

		try {
			serviceGraph.start();
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			Integer integer = injector.getInstance(Key.get(new TypeLiteral<Pojo<Integer>>() {})).getObject();
			assertEquals(integer.intValue(), 579);
			serviceGraph.stop();
		}
	}
}