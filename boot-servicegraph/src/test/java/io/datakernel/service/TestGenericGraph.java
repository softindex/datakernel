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

package io.datakernel.service;

import io.datakernel.di.Injector;
import io.datakernel.di.Key;
import io.datakernel.di.annotation.Named;
import io.datakernel.di.annotation.Provides;
import io.datakernel.di.module.AbstractModule;
import io.datakernel.service.adapter.ServiceAdapters.SimpleServiceAdapter;
import io.datakernel.worker.WorkerPool;
import io.datakernel.worker.WorkerPoolModule;
import io.datakernel.worker.WorkerPools;
import io.datakernel.worker.annotation.Worker;
import org.junit.Test;

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
			install(ServiceGraphModule.create()
					.register(Pojo.class, new SimpleServiceAdapter<Pojo>(false, false) {
						@Override
						protected void start(Pojo instance) {
							System.out.println("...starting " + instance + " : " + instance.object);
						}

						@Override
						protected void stop(Pojo instance) {
							System.out.println("...stopping " + instance + " : " + instance.object);
						}
					}));
			install(WorkerPoolModule.create());
		}

		@Provides
		Pojo integerPojo(WorkerPool workerPool) {
			WorkerPool.Instances<Pojo> list = workerPool.getInstances(Key.ofName(Pojo.class, "worker"));
			WorkerPool.Instances<Pojo> listOther = workerPool.getInstances(Key.ofName(Pojo.class, "anotherWorker"));
			return new Pojo("root");
		}

		@Provides
		WorkerPool pool(WorkerPools workerPools) {
			return workerPools.createPool(WORKERS);
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
		Injector injector = Injector.of(new TestModule());
		injector.getInstance(Pojo.class);

		ServiceGraph serviceGraph = injector.getInstance(ServiceGraph.class);

		try {
			serviceGraph.startFuture().get();
		} finally {
			serviceGraph.stopFuture().get();
		}
	}
}
