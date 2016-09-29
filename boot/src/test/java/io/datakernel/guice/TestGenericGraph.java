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
import com.google.inject.name.Named;
import com.google.inject.name.Names;
import io.datakernel.service.Service;
import io.datakernel.service.ServiceAdapter;
import io.datakernel.service.ServiceGraph;
import io.datakernel.service.ServiceGraphModule;
import io.datakernel.worker.Worker;
import io.datakernel.worker.WorkerPool;
import org.junit.Test;

import java.util.List;
import java.util.concurrent.Executor;

import static org.junit.Assert.assertEquals;

public class TestGenericGraph {
	public static final int WORKERS = 1;

	public static class Pojo<T> {
		private final T object;

		public Pojo(T object) {
			this.object = object;
		}

		public T getObject() {
			return object;
		}
	}

	public static class TestModule extends AbstractModule {

		@Override
		protected void configure() {
			install(ServiceGraphModule.defaultInstance()
					.register(Pojo.class, new ServiceAdapter<Pojo>() {
						@Override
						public Service toService(Pojo instance, Executor executor) {
							return TestServiceGraphServices.immediateService();
						}
					}));
		}

		@Provides
		@Singleton
		WorkerPool workerPool() {
			return new WorkerPool(WORKERS);
		}

		@Provides
		@Singleton
		Pojo<Integer> integerPojo(WorkerPool workerPool) {
			List<Pojo<String>> list = workerPool.getInstances(new TypeLiteral<Pojo<String>>() {});
			List<Pojo<String>> listOther = workerPool.getInstances(Key.get(new TypeLiteral<Pojo<String>>() {}, Names.named("other")));
			return new Pojo<>(Integer.valueOf(listOther.get(0).getObject())
					+ Integer.valueOf(list.get(0).getObject()));
		}

		@Provides
		@Worker
		Pojo<String> stringPojo(@Named("other") Pojo<String> stringPojo) {
			return new Pojo<>("123");
		}

		@Provides
		@Worker
		@Named("other")
		Pojo<String> stringPojoOther() {
			return new Pojo<>("456");
		}
	}

	@Test
	public void test() throws Exception {
		Injector injector = Guice.createInjector(new TestModule());
		ServiceGraph serviceGraph = injector.getInstance(ServiceGraph.class);

		try {
			serviceGraph.startFuture().get();
		} finally {
			Integer integer = injector.getInstance(Key.get(new TypeLiteral<Pojo<Integer>>() {})).getObject();
			assertEquals(integer.intValue(), 579);
			serviceGraph.stopFuture().get();
		}
	}
}