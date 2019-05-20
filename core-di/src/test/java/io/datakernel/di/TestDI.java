package io.datakernel.di;

import io.datakernel.di.module.AbstractModule;
import io.datakernel.di.module.Provides;
import io.datakernel.util.TypeT;
import io.datakernel.util.ref.RefInt;
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

		module.getBindings().values().forEach(bindings -> System.out.println(bindings.iterator().next().getDisplayString()));

		Injector injector = Injector.create(module);

		assertEquals("str: 42", injector.getInstance(String.class));
	}

	@Test
	public void singleton() {
		Injector injector = Injector.create(new AbstractModule() {{
			RefInt ref = new RefInt(41);
			bind(Integer.class).to(ref::inc);
			bind(String.class).to(i -> "str: " + i, Integer.class);
		}});

		assertEquals("str: 42", injector.getInstance(String.class));
		assertEquals("str: 42", injector.getInstance(String.class));
		assertEquals("str: 42", injector.getInstance(String.class));
	}

	@Test
	public void provider() {
		Injector injector = Injector.create(new AbstractModule() {{
			RefInt ref = new RefInt(41);
			bind(Integer.class).to(ref::inc);
			bind(String.class).to(i -> "str: " + i.provideNew(), Key.of(new TypeT<Provider<Integer>>() {}));
		}});

		assertEquals("str: 42", injector.getInstance(String.class));
		assertEquals("str: 42", injector.getInstance(String.class));
		assertEquals("str: 42", injector.getInstance(String.class));

		Provider<String> provider = injector.getInstance(Key.of(new TypeT<Provider<String>>() {}));
		assertEquals("str: 43", provider.provideNew());
		assertEquals("str: 44", provider.provideNew());
		assertEquals("str: 45", provider.provideNew());

		// the first getInstance call was cached, non-pure mutability affects results only when using .provideNew
		assertEquals("str: 42", injector.getInstance(String.class));
	}

	@Test
	public void dsl() {
		Injector injector = Injector.create(new AbstractModule() {

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
		Injector injector = Injector.create(new AbstractModule() {

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

		Injector injector = Injector.create(new AbstractModule() {

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

		Injector injector = Injector.create(new AbstractModule() {

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

	@Test
	public void optional() {
		class ClassWithCustomDeps {

			@Inject(optional = true)
			@Nullable
			String string;

			@Inject
			Integer integer;
		}

		Injector injector = Injector.create(new AbstractModule() {

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

		Injector injector2 = Injector.create(new AbstractModule() {

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

		Injector injector3 = Injector.create(new AbstractModule() {

			@Provides
			ClassWithCustomDeps provide() {
				return new ClassWithCustomDeps();
			}

			@Provides
			String provide2() {
				return "str";
			}
		});

		try {
			injector3.getInstance(ClassWithCustomDeps.class);
			fail("should've failed, but didn't");
		} catch (RuntimeException e) {
			assertEquals("cannot construct", e.getMessage());
		}
	}

	@Test
	public void crossmodule() {
		Injector injector = Injector.create(
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
		Injector injector = Injector.create(override(
				new AbstractModule() {{
					bind(Integer.class).toInstance(17);
					bind(String.class).to(i -> "str: " + i, Integer.class);
				}},
				new AbstractModule() {{
					bind(Integer.class).toInstance(42);
				}}));

		assertEquals("str: 42", injector.getInstance(String.class));
	}

	@Test
	public void simpleGeneric() {

		@SuppressWarnings("unused")
		class Container<Z, T, U> {

			@Inject
			T something;

			@Inject
			U somethingElse;
		}
		AbstractModule module = new AbstractModule() {
			@Override
			protected void configure() {
				bind(String.class).toInstance("hello");
				bind(Integer.class).toInstance(42);
			}

			@Provides
			Container<Float, String, Integer> provide() {
				return new Container<>();
			}
		};

		module.getBindings().values().forEach(binding -> System.out.println(binding.iterator().next().getDisplayString()));

		Injector injector = Injector.create(module);

		Container<Float, String, Integer> instance = injector.getInstance(Key.of(new TypeT<Container<Float, String, Integer>>() {}));
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

		module.getBindings().values().forEach(b -> System.out.println(b.iterator().next().getDisplayString()));

		Injector injector = Injector.create(module);

		assertEquals("hello", injector.getInstance(Key.of(new TypeT<Container<String>>() {})).object);
		assertEquals(42, injector.getInstance(Key.of(new TypeT<Container<Integer>>() {})).object.intValue());
	}
}
