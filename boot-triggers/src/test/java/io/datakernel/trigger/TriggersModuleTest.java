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

import io.datakernel.common.ref.RefBoolean;
import io.datakernel.di.Injector;
import io.datakernel.di.Key;
import io.datakernel.di.annotation.Named;
import io.datakernel.di.annotation.Provides;
import io.datakernel.di.module.AbstractModule;
import io.datakernel.eventloop.Eventloop;
import io.datakernel.launcher.LauncherService;
import io.datakernel.worker.WorkerPool;
import io.datakernel.worker.WorkerPoolModule;
import io.datakernel.worker.WorkerPools;
import io.datakernel.worker.annotation.Worker;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.util.Set;

import static org.junit.Assert.*;

public class TriggersModuleTest {
	@Rule
	public ExpectedException exception = ExpectedException.none();

	@Test
	public void testDuplicatesRejection() {
		exception.expect(IllegalArgumentException.class);
		exception.expectMessage("Cannot assign duplicate trigger");
		Injector.of(
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
				WorkerPoolModule.create(),
				new AbstractModule() {
					int counter = 0;

					@Provides
					@Named("first")
					WorkerPool firstPool(WorkerPools workerPools) {
						return workerPools.createPool(firstPoolSize);
					}

					@Provides
					@Named("second")
					WorkerPool secondPool(WorkerPools workerPools) {
						return workerPools.createPool(secondPoolSize);
					}

					@Provides
					@Worker
					String worker() {
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
		injector.getInstance(Key.ofName(WorkerPool.class, "first")).getInstances(String.class);
		injector.getInstance(Key.ofName(WorkerPool.class, "second")).getInstances(String.class);
		for (LauncherService service : injector.getInstance(new Key<Set<LauncherService>>() {})) {
			service.start().get();
		}
		RefBoolean wasExecuted = new RefBoolean(false);
		try {
			Triggers triggersWatcher = injector.getInstance(Triggers.class);
			assertEquals(firstPoolSize + secondPoolSize, triggersWatcher.getResults().size());
			triggersWatcher.getResults()
					.forEach(triggerWithResult -> assertTrue(triggerWithResult.toString().startsWith("HIGH : String : test :: ")));
			wasExecuted.set(true);
		} finally {
			assertTrue(wasExecuted.get());
		}
	}
}
