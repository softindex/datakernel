package io.datakernel.di.module;

import io.datakernel.di.annotation.Named;
import io.datakernel.di.annotation.Provides;
import io.datakernel.di.annotation.ProvidesIntoSet;
import io.datakernel.di.core.Injector;
import io.datakernel.di.core.Key;
import org.junit.Assert;
import org.junit.Test;

import java.util.function.Consumer;

import static org.junit.Assert.assertEquals;

public class InstanceConsumerModuleTest {
	@Test
	public void consumerTransformerHookupTest() {
		AbstractModule module = new AbstractModule() {
			@ProvidesIntoSet
			Consumer<String> consumer() {
				return System.out::println;
			}

			@ProvidesIntoSet
			Consumer<String> consumer2() {
				return System.err::println;
			}

			@Provides
			String string() { return "Hello, World"; }
		};

		Module module1 = Module.create()
				.install(module)
				.combineWith(InstanceConsumerModule.create()
						.withPriority(99));

		Injector injector = Injector.of(module1);
		String instance = injector.getInstance(String.class);
		Assert.assertEquals("Hello, World", instance);
	}

	//[START REGION_1]
	@Test
	public void consumerTransformerHookupWithNameTest() {
		int[] calls = {0};
		AbstractModule module = new AbstractModule() {
			@Named("consumer1")
			@ProvidesIntoSet
			Consumer<String> consumer() {
				return str -> {
					System.out.println(str);
					System.out.println(++calls[0]);
				};
			}

			@ProvidesIntoSet
			Consumer<String> consumer2() {
				return str -> {
					System.err.println(str);
					System.out.println(++calls[0]);
				};
			}

			@Named("consumer1")
			@Provides
			String string() { return "Hello, World"; }
		};

		Module module1 = Module.create()
				.install(module)
				.install(InstanceConsumerModule.create()
						.withPriority(99));

		Injector injector = Injector.of(module1);
		String instance = injector.getInstance(Key.of(String.class, "consumer1"));
		assertEquals(instance, "Hello, World");
		assertEquals(1, calls[0]);
	}
	//[END REGION_1]
}
