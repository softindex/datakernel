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
import io.datakernel.service.ServiceAdapters.SimpleServiceAdapter;
import io.datakernel.service.ServiceGraph;
import io.datakernel.service.ServiceGraphModule;
import io.datakernel.worker.Worker;
import io.datakernel.worker.WorkerPool;
import org.junit.Test;

import java.util.List;

import static com.google.inject.name.Names.named;

public class TestGenericGraph {
	public static final int WORKERS = 4;

	public static class Pojo {
		private final String object;

		public Pojo(String object) {
			this.object = object;
		}

	}

	public static class TestModule extends AbstractModule {
		@Override
		protected void configure() {
			install(ServiceGraphModule.defaultInstance()
					.register(Pojo.class, new SimpleServiceAdapter<Pojo>(false, false) {
						@Override
						protected void start(Pojo instance) throws Exception {
							System.out.println("...starting " + instance + " : " + instance.object);
						}

						@Override
						protected void stop(Pojo instance) throws Exception {
							System.out.println("...stopping " + instance + " : " + instance.object);
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
		Pojo integerPojo(WorkerPool workerPool) {
			List<Pojo> list = workerPool.getInstances(Key.get(Pojo.class, named("worker")));
			List<Pojo> listOther = workerPool.getInstances(Key.get(Pojo.class, named("anotherWorker")));
			return new Pojo("root");
		}

		@Provides
		@Worker
		@Named("worker")
		Pojo stringPojoOther() {
			return new Pojo("worker");
		}

		@Provides
		@Worker
		@Named("anotherWorker")
		Pojo stringPojo(@Named("worker") Pojo worker) {
			return new Pojo("anotherWorker");
		}
	}

	@Test
	public void test() throws Exception {
		Injector injector = Guice.createInjector(Stage.PRODUCTION, new TestModule());
		Provider<ServiceGraph> serviceGraphProvider = injector.getProvider(ServiceGraph.class);
		ServiceGraph serviceGraph = serviceGraphProvider.get();

		try {
			serviceGraph.startFuture().get();
		} finally {
			Pojo root = injector.getInstance(Pojo.class);
			serviceGraph.stopFuture().get();
		}
	}
}