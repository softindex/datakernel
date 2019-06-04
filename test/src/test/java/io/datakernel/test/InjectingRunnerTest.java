package io.datakernel.test;

import io.datakernel.di.*;
import io.datakernel.di.module.AbstractModule;
import io.datakernel.di.module.Provides;
import org.junit.Test;
import org.junit.runner.RunWith;

import java.util.List;

import static java.util.Arrays.asList;
import static junit.framework.TestCase.assertEquals;

@RunWith(InjectingRunner.class)
@InjectingRunner.UseModules(InjectingRunnerTest.TestModule.class)
public final class InjectingRunnerTest {

	static class TestModule extends AbstractModule {

		@Provides
		String string() {
			return "hello";
		}

		@Provides
		Integer integer() {
			return 42;
		}

		@Provides
		<T> List<T> tripleList(T instance) {
			return asList(instance, instance, instance);
		}
	}

	@Inject
	List<String> hellos;

	@Inject
	List<Integer> numbers;

	@Provides
	@Named("test")
	String string() {
		return "string";
	}

	@Inject
	Injector injector;

	@Test
	public void test1() {
		assertEquals(asList("hello", "hello", "hello"), hellos);
		assertEquals(asList(42, 42, 42), numbers);

		assertEquals("string", injector.getInstance(Key.of(String.class, Name.of("test"))));
	}
}
