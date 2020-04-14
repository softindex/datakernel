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

import io.datakernel.di.annotation.Named;
import io.datakernel.di.annotation.Provides;
import io.datakernel.di.core.Injector;
import io.datakernel.di.core.Key;
import io.datakernel.di.module.AbstractModule;
import org.hamcrest.core.IsSame;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

public class ServiceGraphTest {
	@Rule
	public final ExpectedException expected = ExpectedException.none();

	@Test
	public void testProperClosingForFailingServiceOneComponent() throws Exception {
		Injector injector = Injector.of(new FailingModule());
		injector.getInstance(Key.of(BlockingService.class, "TopService1"));
		ServiceGraph graph = injector.getInstance(ServiceGraph.class);
		expected.expectCause(IsSame.sameInstance(FailingModule.INTERRUPTED));
		graph.startFuture().get();
	}

	@Test
	public void testProperClosingForFailingServiceTwoComponents() throws Exception {
		Injector injector = Injector.of(new FailingModule());
		injector.getInstance(Key.of(BlockingService.class, "TopService1"));
		injector.getInstance(Key.of(BlockingService.class, "TopService2"));
		ServiceGraph graph = injector.getInstance(ServiceGraph.class);
		expected.expectCause(IsSame.sameInstance(FailingModule.INTERRUPTED));
		graph.startFuture().get();
	}

	// region modules
	public static class FailingModule extends AbstractModule {
		public static final io.datakernel.common.exception.ExpectedException INTERRUPTED = new io.datakernel.common.exception.ExpectedException("interrupted");

		@Override
		protected void configure() {
			install(ServiceGraphModule.create());
		}

		@Provides
		@Named("FailService")
		BlockingService failService() {
			return new BlockingServiceEmpty() {
				@Override
				public void start() throws Exception{
					throw INTERRUPTED;
				}
			};
		}

		@Provides
		@Named("TopService1")
		BlockingService service1(@Named("FailService") BlockingService failService) {
			return new BlockingServiceEmpty();
		}

		@Provides
		@Named("TopService2")
		BlockingService service2(@Named("FailService") BlockingService failService) {
			return new BlockingServiceEmpty();
		}
	}

	public static class BlockingServiceEmpty implements BlockingService {
		@Override
		public void start() throws Exception {
		}

		@Override
		public void stop() {
		}
	}
	// endregion
}
