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

import com.google.inject.*;
import com.google.inject.name.Named;
import com.google.inject.name.Names;
import io.datakernel.async.Promise;
import io.datakernel.eventloop.Eventloop;
import io.datakernel.eventloop.EventloopService;
import org.hamcrest.core.IsSame;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

public class ServiceGraphTest {
	@Rule
	public final ExpectedException expected = ExpectedException.none();

	@Test
	public void testProperClosingForFailingServiceOneComponent() throws Exception {
		Injector injector = Guice.createInjector(new FailingModule());
		injector.getInstance(Key.get(EventloopService.class, Names.named("TopService1")));
		ServiceGraph graph = injector.getInstance(ServiceGraph.class);
		expected.expectCause(IsSame.sameInstance(FailingModule.INTERRUPTED));
		graph.startFuture().get();
	}

	@Test
	public void testProperClosingForFailingServiceTwoComponents() throws Exception {
		Injector injector = Guice.createInjector(new FailingModule());
		injector.getInstance(Key.get(EventloopService.class, Names.named("TopService1")));
		injector.getInstance(Key.get(EventloopService.class, Names.named("TopService2")));
		ServiceGraph graph = injector.getInstance(ServiceGraph.class);
		expected.expectCause(IsSame.sameInstance(FailingModule.INTERRUPTED));
		graph.startFuture().get();
	}

	// region modules
	public static class FailingModule extends AbstractModule {
		public static final InterruptedException INTERRUPTED = new InterruptedException("interrupted");

		@Override
		protected void configure() {
			install(ServiceGraphModule.defaultInstance());
		}

		@Provides
		@Singleton
		Eventloop eventloop() {
			return Eventloop.create();
		}

		@Provides
		@Singleton
		@Named("FailService")
		EventloopService failService(Eventloop eventloop) {
			return new EventloopServiceEmpty(eventloop) {
				@Override
				public Promise<Void> start() {
					return Promise.ofException(INTERRUPTED);
				}
			};
		}

		@Provides
		@Singleton
		@Named("TopService1")
		EventloopService service1(Eventloop eventloop, @Named("FailService") EventloopService failService) {
			return new EventloopServiceEmpty(eventloop);
		}

		@Provides
		@Singleton
		@Named("TopService2")
		EventloopService service2(Eventloop eventloop, @Named("FailService") EventloopService failService) {
			return new EventloopServiceEmpty(eventloop);
		}
	}

	public static class EventloopServiceEmpty implements EventloopService {
		private final Eventloop eventloop;

		EventloopServiceEmpty(Eventloop eventloop) {this.eventloop = eventloop;}

		@Override
		public Eventloop getEventloop() {
			return eventloop;
		}

		@Override
		public Promise<Void> start() {
			return Promise.complete();
		}

		@Override
		public Promise<Void> stop() {
			return Promise.complete();
		}

	}
	// endregion
}
