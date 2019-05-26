package io.datakernel.di;

import io.datakernel.di.module.AbstractModule;
import io.datakernel.di.module.Module;
import io.datakernel.di.module.Provides;
import org.jetbrains.annotations.Nullable;
import org.junit.Ignore;
import org.junit.Test;

import static io.datakernel.di.module.Modules.override;
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
			bind(new Key<Provider<String>>() {}).require();
		}});

		assertEquals("str: 42", injector.getInstance(String.class));
		assertEquals("str: 42", injector.getInstance(String.class));
		assertEquals("str: 42", injector.getInstance(String.class));

		Provider<String> provider = injector.getInstance(new Key<Provider<String>>() {});
		assertEquals("str: 43", provider.provideNew());
		assertEquals("str: 44", provider.provideNew());
		assertEquals("str: 45", provider.provideNew());

		// the first getInstance call was cached, non-pure mutability affects results only when using .provideNew
		assertEquals("str: 42", injector.getInstance(String.class));
	}

	@Test
	public void dsl() {
		Injector injector = Injector.of(new AbstractModule() {

			@Provides
			String provide(Integer integer) {
				return "str: " + integer;
			}

			@Provides
			Integer provide() {
				return 42;
			}
		});

		assertEquals("str: 42", injector.getInstance(String.class));
	}

	@Test
	public void namedDsl() {
		Injector injector = Injector.of(new AbstractModule() {

			@Provides
			String provide(@Named("test") Integer integer) {
				return "str: " + integer;
			}

			@Provides
			@Named("test")
			Integer provide() {
				return 42;
			}

			@Provides
			@Named("test2")
			Integer provide2() {
				return 43;
			}

			@Provides
			Integer provide0() {
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
			ClassWithCustomDeps provide() {
				return new ClassWithCustomDeps();
			}

			@Provides
			@Named("test")
			String provide(Integer integer) {
				return "str: " + integer;
			}

			@Provides
			Integer provide2() {
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
			Inherited provide() {
				return new Inherited();
			}

			@Provides
			String provide(Integer integer) {
				return "str: " + integer;
			}

			@Provides
			Integer provide2() {
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
				bind(RecursiveA.class).require();
			}};

			System.out.println(module.getBindingsMultimap());

			Injector injector = Injector.of(module);
			fail("should've detected the cycle and fail");
		} catch (RuntimeException e) {
			e.printStackTrace();
		}
	}

	@Test
	public void optional() {
		class ClassWithCustomDeps {

			@Inject(optional = true)
			@Nullable
			String string;

			@Inject
			Integer integer;
		}

		Injector injector = Injector.of(new AbstractModule() {

			@Provides
			ClassWithCustomDeps provide() {
				return new ClassWithCustomDeps();
			}

			@Provides
			Integer provide2() {
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
			assertEquals("unsatisfied dependency java.lang.Integer with no implicit bindings", e.getMessage());
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
			bind(new Key<Container<Float, String, Integer>>() {}).require();
		}};

//		module.getBindings().values().forEach(binding -> System.out.println(binding.iterator().next().getDisplayString()));

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
			<T> Container<T> get(T t) {
				return new Container<>(t);
			}

			@Provides
			String provide() {
				return "hello";
			}

			@Provides
			Integer provide2() {
				return 42;
			}
		};

//		module.getBindings().values().forEach(b -> System.out.println(b.iterator().next().getDisplayString()));

		Injector injector = Injector.of(module);

		assertEquals("hello", injector.getInstance(new Key<Container<String>>() {}).object);
		assertEquals(42, injector.getInstance(new Key<Container<Integer>>() {}).object.intValue());
	}
}
