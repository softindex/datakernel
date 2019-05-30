package io.datakernel.di;

import io.datakernel.di.module.AbstractModule;
import io.datakernel.di.module.Module;
import io.datakernel.di.module.Provides;
import io.datakernel.di.module.ProvidesIntoSet;
import org.jetbrains.annotations.Nullable;
import org.junit.Ignore;
import org.junit.Test;

import java.util.List;
import java.util.Set;
import java.util.stream.Stream;

import static io.datakernel.di.module.Modules.combine;
import static io.datakernel.di.module.Modules.override;
import static java.util.Collections.singletonList;
import static java.util.stream.Collectors.toSet;
import static org.junit.Assert.*;

public final class TestDI {

	@Test
	public void basic() {
		AbstractModule module = new AbstractModule() {{
			bind(Integer.class).toInstance(42);
			bind(String.class).to(i -> "str: " + i, Integer.class);
		}};

//		module.getBindings().values().forEach(bindings -> System.out.println(bindings.iterator().next().getDisplayString()));

		Injector injector = Injector.of(module);

		assertEquals("str: 42", injector.getInstance(String.class));
	}

	@Test
	public void singleton() {
		Injector injector = Injector.of(new AbstractModule() {{
			int[] ref = new int[]{41};
			bind(Integer.class).to(() -> ++ref[0]);
			bind(String.class).to(i -> "str: " + i, Integer.class);
		}});

		assertEquals("str: 42", injector.getInstance(String.class));
		assertEquals("str: 42", injector.getInstance(String.class));
		assertEquals("str: 42", injector.getInstance(String.class));
	}

	@Test
	public void provider() {
		Injector injector = Injector.of(new AbstractModule() {{
			int[] ref = new int[]{41};
			bind(Integer.class).to(() -> ++ref[0]);
			bind(String.class).to(i -> "str: " + i.provideNew(), new Key<Provider<Integer>>() {});
			bind(new Key<Provider<String>>() {}).implicitly();
		}});

		// the provideNew call calls the integer factory for the second time (first was for the singleton/cache), so it returns 43
		// nobody should make non-pure factories anyway, this is just for testing
		assertEquals("str: 43", injector.getInstance(String.class));
		assertEquals("str: 43", injector.getInstance(String.class));
		assertEquals("str: 43", injector.getInstance(String.class));

		Provider<String> provider = injector.getInstance(new Key<Provider<String>>() {});
		assertEquals("str: 44", provider.provideNew());
		assertEquals("str: 45", provider.provideNew());
		assertEquals("str: 46", provider.provideNew());

		// the first getInstance call was cached, non-pure mutability affects results only when using .provideNew
		assertEquals("str: 43", injector.getInstance(String.class));

		// as was said in a big comment above first assert
		assertEquals(42, injector.getInstance(Integer.class).intValue());
	}

	@Test
	public void dsl() {
		Injector injector = Injector.of(new AbstractModule() {

			@Provides
			String string(Integer integer) {
				return "str: " + integer;
			}

			@Provides
			Integer integer() {
				return 42;
			}
		});

		assertEquals("str: 42", injector.getInstance(String.class));
	}

	@Test
	public void namedDsl() {
		Injector injector = Injector.of(new AbstractModule() {

			@Provides
			String string(@Named("test") Integer integer) {
				return "str: " + integer;
			}

			@Provides
			@Named("test")
			Integer integer1() {
				return 42;
			}

			@Provides
			@Named("test2")
			Integer integer2() {
				return 43;
			}

			@Provides
			Integer integer() {
				return -1;
			}
		});

		assertEquals("str: 42", injector.getInstance(String.class));
	}

	@Test
	public void injectDsl() {
		class ClassWithCustomDeps {

			@Inject
			@Named("test")
			String string;

			@Inject
			Integer raw;
		}

		Injector injector = Injector.of(new AbstractModule() {

			@Provides
			ClassWithCustomDeps classWithCustomDeps() {
				return new ClassWithCustomDeps();
			}

			@Provides
			@Named("test")
			String testString(Integer integer) {
				return "str: " + integer;
			}

			@Provides
			Integer integer() {
				return 42;
			}
		});

		ClassWithCustomDeps instance = injector.getInstance(ClassWithCustomDeps.class);
		assertEquals("str: 42", instance.string);
		assertEquals(42, instance.raw.intValue());
	}

	@Test
	public void inheritedInjects() {
		class ClassWithCustomDeps {

			@Inject
			String string;
		}

		class Inherited extends ClassWithCustomDeps {
		}

		Injector injector = Injector.of(new AbstractModule() {

			@Provides
			Inherited inherited() {
				return new Inherited();
			}

			@Provides
			String string(Integer integer) {
				return "str: " + integer;
			}

			@Provides
			Integer integer() {
				return 42;
			}
		});

		Inherited instance = injector.getInstance(Inherited.class);
		assertEquals("str: 42", instance.string);
	}

	@Inject
	static class RecursiveA {

		@Inject
		RecursiveB dependency;
	}

	@Inject
	static class RecursiveB {

		@Inject
		RecursiveA dependency;
	}

	@Test
	public void cyclicInjects() {
		try {
			Module module = new AbstractModule() {{
				bind(RecursiveA.class).implicitly();
			}};

			module.getBindings().get().forEach((key, binding) ->
					System.out.println(key.getDisplayString() + " -> " + binding.getDisplayString()));

			Injector.of(module);
			fail("should've detected the cycle and fail");
		} catch (RuntimeException e) {
			e.printStackTrace();
		}
	}

	@Test
	public void optionalInjects() {
		class ClassWithCustomDeps {

			@Inject(optional = true)
			@Nullable
			String string;

			@Inject
			Integer integer;
		}

		Injector injector = Injector.of(new AbstractModule() {

			@Provides
			ClassWithCustomDeps classWithCustomDeps() {
				return new ClassWithCustomDeps();
			}

			@Provides
			Integer integer() {
				return 42;
			}
		});

		ClassWithCustomDeps instance = injector.getInstance(ClassWithCustomDeps.class);
		assertNull(instance.string);
		assertEquals(42, instance.integer.intValue());

		Injector injector2 = Injector.of(new AbstractModule() {

			@Provides
			ClassWithCustomDeps provide() {
				return new ClassWithCustomDeps();
			}

			@Provides
			Integer provide2() {
				return 42;
			}

			@Provides
			String provide3() {
				return "str";
			}
		});

		ClassWithCustomDeps instance2 = injector2.getInstance(ClassWithCustomDeps.class);
		assertEquals("str", instance2.string);
		assertEquals(42, instance2.integer.intValue());

		try {
			Injector injector3 = Injector.of(new AbstractModule() {

				@Provides
				ClassWithCustomDeps provide() {
					return new ClassWithCustomDeps();
				}

				@Provides
				String provide2() {
					return "str";
				}
			});
			injector3.getInstance(ClassWithCustomDeps.class);
			fail("should've failed, but didn't");
		} catch (RuntimeException e) {
			e.printStackTrace();
			assertTrue(e.getMessage().startsWith("unsatisfied dependencies detected:\n\tkey java.lang.Integer\n\t\trequired at"));
		}
	}

	@Test
	public void crossmodule() {
		Injector injector = Injector.of(
				new AbstractModule() {{
					bind(Integer.class).toInstance(42);
				}},
				new AbstractModule() {{
					bind(String.class).to(i -> "str: " + i, Integer.class);
				}});

		assertEquals("str: 42", injector.getInstance(String.class));
	}

	@Test
	public void overrides() {
		Injector injector = Injector.of(override(
				new AbstractModule() {{
					bind(Integer.class).toInstance(17);
					bind(String.class).to(i -> "str: " + i, Integer.class);
				}},
				new AbstractModule() {{
					bind(Integer.class).toInstance(42);
				}}));

		assertEquals("str: 42", injector.getInstance(String.class));
	}

	@SuppressWarnings("unused")
	@Inject
	static class Container<Z, T, U> {

		@Inject
		T something;

		@Inject
		U somethingElse;
	}

	@Test
	public void simpleGeneric() {

		AbstractModule module = new AbstractModule() {{
			bind(String.class).toInstance("hello");
			bind(Integer.class).toInstance(42);
			bind(new Key<Container<Float, String, Integer>>() {}).implicitly();
		}};

		Injector injector = Injector.of(module);

		Container<Float, String, Integer> instance = injector.getInstance(new Key<Container<Float, String, Integer>>() {});
		assertEquals("hello", instance.something);
		assertEquals(42, instance.somethingElse.intValue());
	}

	@Test
	@Ignore
	public void container() {

		class Container<T> {
			private final T object;

			public Container(T object) {
				this.object = object;
			}
		}

		AbstractModule module = new AbstractModule() {
			@Provides
			<T> Container<T> container(T t) {
				return new Container<>(t);
			}

			@Provides
			String string() {
				return "hello";
			}

			@Provides
			Integer integer() {
				return 42;
			}
		};

		Injector injector = Injector.of(module);

		assertEquals("hello", injector.getInstance(new Key<Container<String>>() {}).object);
		assertEquals(42, injector.getInstance(new Key<Container<Integer>>() {}).object.intValue());
	}

	@Test
	public void providesDuplicated() {
		try {
			Injector.of(new AbstractModule() {{
				bind(Integer.class).toInstance(42);
				bind(String.class).to(i -> "str1: " + i, Integer.class);
				bind(String.class).to(i -> "str2: " + i, Integer.class);
			}});
			fail("should've failed");
		} catch (RuntimeException e) {
			e.printStackTrace();
			assertTrue(e.getMessage().startsWith("Duplicate bindings for key java.lang.String"));
		}
	}

	@Test
	public void providesOptional() {
		Module module = new AbstractModule() {
			@Provides
			String string(Integer integer, @Optional Float f) {
				return "str: " + integer + ", " + f;
			}

			@Provides
			Integer integer() {
				return 42;
			}
		};
		Injector injector = Injector.of(module);

		assertEquals("str: 42, null", injector.getInstance(String.class));

		Injector injector2 = Injector.of(combine(module, new AbstractModule() {{
			bind(Float.class).toInstance(3.14f);
		}}));

		assertEquals("str: 42, 3.14", injector2.getInstance(String.class));
	}

	@Test
	public void providesIntoSet() {
		Injector injector = Injector.of(new AbstractModule() {
			@Provides
			Integer integer() {
				return 42;
			}

			@ProvidesIntoSet
			String string1(Integer integer) {
				return "str1: " + integer;
			}

			@ProvidesIntoSet
			String string2(Integer integer) {
				return "str2: " + integer;
			}

			@ProvidesIntoSet
			String string3(Integer integer) {
				return "str3: " + integer;
			}

			@ProvidesIntoSet
			List<String> stringsB1(Integer integer) {
				return singletonList("str1: " + integer);
			}

			@ProvidesIntoSet
			List<String> stringsB2(Integer integer) {
				return singletonList("str2: " + integer);
			}

		});

		Set<String> instance = injector.getInstance(new Key<Set<String>>() {});

		Set<String> expected = Stream.of("str1: 42", "str2: 42", "str3: 42").collect(toSet());


		assertEquals(expected, instance);

		Key<Set<List<String>>> key = new Key<Set<List<String>>>() {};
		Set<List<String>> instance2 = injector.getInstance(key);

		Set<List<String>> expected2 = Stream.of(singletonList("str1: 42"), singletonList("str2: 42")).collect(toSet());

		assertEquals(expected2, instance2);
	}


	@Test
	public void hierarchyModules() {
		class Module1 extends AbstractModule {
			@Provides
			Integer integer() {
				return 123;
			}
		}

		class Module2 extends Module1 {
			@Provides
			String string(Integer integer) {
				return integer.toString();
			}
		}

		Injector injector = Injector.of(new Module2());
		String string = injector.getInstance(String.class);

		assertEquals("123", string);
	}

	@Test
	public void moduleWithGenerics() {
		abstract class Module1<D> extends AbstractModule {
			@Provides
			String string(D object) {
				return "str: " + object.toString();
			}
		}

		Injector injector = Injector.of(new Module1<Integer>() {{
			bind(Integer.class).toInstance(42);
		}});
		String string = injector.getInstance(String.class);

		assertEquals("str: 42", string);
	}

}
