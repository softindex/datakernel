package io.datakernel.test;

import io.datakernel.di.Injector;
import io.datakernel.di.Key;
import io.datakernel.di.Qualifiers;
import io.datakernel.di.annotation.Inject;
import io.datakernel.di.annotation.Named;
import io.datakernel.di.annotation.Provides;
import io.datakernel.di.module.AbstractModule;
import io.datakernel.test.DatakernelRunnerTest.ClassModule;
import org.jetbrains.annotations.Nullable;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;

import java.util.List;

import static java.util.Arrays.asList;
import static junit.framework.TestCase.assertEquals;
import static org.junit.Assert.assertNotNull;

@RunWith(DatakernelRunner.class)
@UseModules({ClassModule.class})
public class DatakernelRunnerTest {
	private static final String HELLO = "hello";
	private static final String CUSTOM = "custom";
	private static final String PROVIDES_STRING = "string";

	static class ClassModule extends AbstractModule {
		@Provides
		String string() {
			return HELLO;
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
	@Named(PROVIDES_STRING)
	String string() {
		return PROVIDES_STRING;
	}

	@Inject
	Injector injector;

	@Test
	public void testCommon(Injector injector) {
		assertEquals(asList(HELLO, HELLO, HELLO), hellos);
		assertEquals(asList(42, 42, 42), numbers);

		assertEquals(PROVIDES_STRING, injector.getInstance(Key.of(String.class, Qualifiers.named(PROVIDES_STRING))));
	}

	@Test
	public void testProvides(@Named(PROVIDES_STRING) String s) {
		assertEquals(PROVIDES_STRING, s);

		assertEquals(asList(HELLO, HELLO, HELLO), hellos);
	}

	static class TestModule extends AbstractModule {
		@Provides
		@Named(CUSTOM)
		String string() {
			return CUSTOM;
		}
	}

	@Test
	@UseModules({TestModule.class})
	public void testCustom(@Named(CUSTOM) String s2) {
		assertEquals(CUSTOM, s2);

		assertEquals(asList(HELLO, HELLO, HELLO), hellos);
	}

	public static class InjectableClass {
		private final String s;

		@Inject
		public InjectableClass(String s) {
			this.s = s;
		}
	}

	@Test
	public void testInjectable(InjectableClass obj) {
		assertNotNull(obj);
		assertEquals(HELLO, obj.s);
	}

	@RunWith(DatakernelRunner.class)
	public static class TestBefore {

		@Inject
		public static class Setupable {
			@Nullable String info = null;
		}

		public static class Dependant {
			@Nullable String info;

			@Inject
			Dependant(Setupable setupable) {
				info = setupable.info;
			}
		}

		@Before
		public void setup(Setupable setupable) {
			setupable.info = "hello world";
		}

		@Test
		public void testObjectCreationBetweenBeforeAndTest(Dependant obj) {
			assertEquals("hello world", obj.info);
		}
	}
}
