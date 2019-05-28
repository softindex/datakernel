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

package io.datakernel.trigger;

import io.datakernel.di.Injector;
import io.datakernel.di.Key;
import io.datakernel.di.Name;
import io.datakernel.di.Named;
import io.datakernel.di.module.AbstractModule;
import io.datakernel.di.module.Provides;
import io.datakernel.di.module.ProvidesIntoSet;
import io.datakernel.eventloop.Eventloop;
import io.datakernel.service.ServiceGraph;
import io.datakernel.service.ServiceGraphModule;
import io.datakernel.trigger.Triggers.TriggerWithResult;
import io.datakernel.trigger.TriggersModule.TriggersModuleService;
import io.datakernel.util.Initializer;
import io.datakernel.util.ref.RefBoolean;
import io.datakernel.worker.Worker;
import io.datakernel.worker.WorkerPool;
import io.datakernel.worker.WorkerPoolModule;
import io.datakernel.worker.WorkerPools;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.util.List;
import java.util.concurrent.ExecutionException;

import static org.junit.Assert.*;

public class TriggersModuleTest {
	@Rule
	public ExpectedException exception = ExpectedException.none();

	@Test
	public void testDuplicatesRejection() {
		exception.expect(IllegalArgumentException.class);
		exception.expectMessage("Cannot assign duplicate triggers");
		Injector.of(
				ServiceGraphModule.defaultInstance(),
				TriggersModule.create()
						.with(Eventloop.class, Severity.HIGH, "test", eventloop -> TriggerResult.create())
						.with(Eventloop.class, Severity.HIGH, "test", eventloop -> TriggerResult.create())
		);
		fail();
	}

	@Test
	public void testWithSeveralWorkerPools() throws Exception {
		int firstPoolSize = 10;
		int secondPoolSize = 5;
		Injector injector = Injector.of(
				ServiceGraphModule.defaultInstance(),
				new WorkerPoolModule(),
				new AbstractModule() {
					int counter = 0;

					@Provides
					@Named("first")
					WorkerPool provideFirstPool(WorkerPools workerPools) {
						return workerPools.createPool(firstPoolSize);
					}

					@Provides
					@Named("second")
					WorkerPool provideSecondPool(WorkerPools workerPools) {
						return workerPools.createPool(secondPoolSize);
					}

					@Provides
					@Worker
					String provideWorker() {
						return "" + counter++;
					}

					@Provides
					Integer provide(@Named("first") WorkerPool workerPool1, @Named("second") WorkerPool workerPool2) {
						workerPool1.getInstances(String.class);
						workerPool2.getInstances(String.class);
						return 0;
					}
				},
				TriggersModule.create()
						.with(String.class, Severity.HIGH, "test", s -> TriggerResult.create())
		);
		injector.getInstance(Key.of(WorkerPool.class, Name.of("first"))).getInstances(String.class);
		injector.getInstance(Key.of(WorkerPool.class, Name.of("second"))).getInstances(String.class);
		injector.getInstanceOrNull(TriggersModuleService.class);
		ServiceGraph serviceGraph = injector.getInstance(ServiceGraph.class);
		RefBoolean wasExecuted = new RefBoolean(false);
		try {
			serviceGraph.startFuture().get();
			Triggers triggersWatcher = injector.getInstance(Triggers.class);
			assertEquals(firstPoolSize + secondPoolSize, triggersWatcher.getResults().size());
			triggersWatcher.getResults()
					.forEach(triggerWithResult -> assertTrue(triggerWithResult.toString().startsWith("HIGH : String : test :: ")));
			wasExecuted.set(true);
		} finally {
			assertTrue(wasExecuted.get());
			serviceGraph.stopFuture().get();
		}
	}

	@Test
	public void testMultiModule() throws ExecutionException, InterruptedException {
		Injector injector = Injector.of(
				ServiceGraphModule.defaultInstance(),
				new AbstractModule() {
					@Override
					protected void configure() {
						install(TriggersModule.create());
					}

					@Provides
					Eventloop provide() {
						return Eventloop.create();
					}

					@ProvidesIntoSet
					Initializer<TriggersModule> triggersModuleInitializer(Eventloop eventloop) {
						return triggersModule -> triggersModule
								.with(Eventloop.class, Severity.HIGH, "test", $ -> TriggerResult.create());
					}
				},
				new AbstractModule() {
					@ProvidesIntoSet
					Initializer<TriggersModule> triggersModuleInitializer() {
						return triggersModule -> triggersModule
								.with(Eventloop.class, Severity.HIGH, "testModule1", $ -> TriggerResult.create());
					}
				},
				new AbstractModule() {
					@ProvidesIntoSet
					Initializer<TriggersModule> triggersModuleInitializer() {
						return triggersModule -> triggersModule
								.with(Eventloop.class, Severity.HIGH, "testModule2", $ -> TriggerResult.create());
					}
				}
		);
		injector.getInstanceOrNull(TriggersModuleService.class);
		ServiceGraph serviceGraph = injector.getInstance(ServiceGraph.class);
		RefBoolean wasExecuted = new RefBoolean(false);
		try {
			serviceGraph.startFuture().get();
			Triggers triggersWatcher = injector.getInstance(Triggers.class);
			List<TriggerWithResult> triggerResults = triggersWatcher.getResults();
			assertEquals(3, triggerResults.size());
			triggerResults.forEach(result -> assertTrue(result.toString().startsWith("HIGH : Eventloop : test")));
			wasExecuted.set(true);
		} finally {
			assertTrue(wasExecuted.get());
			serviceGraph.stopFuture().get();
		}
	}
}
