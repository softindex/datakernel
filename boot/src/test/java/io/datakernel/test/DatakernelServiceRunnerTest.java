package io.datakernel.test;

import io.datakernel.di.annotation.Named;
import io.datakernel.di.annotation.Provides;
import io.datakernel.di.module.AbstractModule;
import io.datakernel.service.Service;
import io.datakernel.service.ServiceGraphModule;
import io.datakernel.test.DatakernelRunner.UseModules;
import io.datakernel.test.DatakernelServiceRunnerTest.TestModule;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;

import java.util.concurrent.CompletableFuture;

import static org.junit.Assert.*;

@RunWith(DatakernelServiceRunner.class)
@UseModules({TestModule.class})
public class DatakernelServiceRunnerTest {

	private static class TestService implements Service {
		private boolean starting;
		private boolean stopped;

		@Override
		public CompletableFuture<?> start() {
			this.starting = true;
			return CompletableFuture.completedFuture(null);
		}

		@Override
		public CompletableFuture<?> stop() {
			this.stopped = true;
			return CompletableFuture.completedFuture(null);
		}
	}

	public static class TestModule extends AbstractModule {
		@Override
		protected void configure() {
			bind(Service.class).toInstance(new TestService());
			install(ServiceGraphModule.defaultInstance());
		}

		@Provides
		@Named("SecondInstance")
		Service service() {
			return new TestService();
		}
	}

	private Service serviceCopy;

	@Before
	public void before(Service service) {
		serviceCopy = service;
		assertTrue(service instanceof TestService);
		assertFalse(((TestService) service).starting);
		assertFalse(((TestService) service).stopped);
	}

	@Test
	public void test(Service service, @Named("SecondInstance") Service second) {
		assertSame(service, serviceCopy);
		assertNotSame(service, second);
		
		assertTrue(service instanceof TestService);
		assertTrue(((TestService) service).starting);
		assertFalse(((TestService) service).stopped);

		assertTrue(second instanceof TestService);
		assertTrue(((TestService) second).starting);
		assertFalse(((TestService) second).stopped);
	}

	@After
	public void after(Service service) {
		assertTrue(service instanceof TestService);
		assertTrue(((TestService) service).starting);
		assertTrue(((TestService) service).stopped);
	}

	@RunWith(DatakernelServiceRunner.class)
	public static class WithoutModulesTest {
		@Test
		public void test() {
			assertTrue(true);
		}
	}

	@RunWith(DatakernelServiceRunner.class)
	public static class BeforeModulesTest {
		private Service testService;

		@Before
		@UseModules({TestModule.class})
		public void before(Service service) {
			assertTrue(service instanceof TestService);
			assertFalse(((TestService) service).starting);
			assertFalse(((TestService) service).stopped);
			testService = service;
		}

		@Test
		public void test() {
			assertNotNull(testService);
			assertTrue(testService instanceof TestService);
			assertTrue(((TestService) testService).starting);
			assertFalse(((TestService) testService).stopped);
		}
	}
}
