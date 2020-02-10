package io.datakernel.di;

import io.datakernel.di.annotation.Optional;
import io.datakernel.di.annotation.*;
import io.datakernel.di.core.*;
import io.datakernel.di.module.AbstractModule;
import io.datakernel.di.module.Module;
import io.datakernel.di.module.Modules;
import io.datakernel.di.util.Constructors.Constructor0;
import io.datakernel.di.util.Trie;
import io.datakernel.di.util.Utils;
import org.jetbrains.annotations.Nullable;
import org.junit.Test;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Stream;

import static io.datakernel.di.module.Modules.combine;
import static io.datakernel.di.module.Modules.override;
import static io.datakernel.di.util.Utils.printGraphVizGraph;
import static java.util.Arrays.asList;
import static java.util.Collections.*;
import static java.util.stream.Collectors.toSet;
import static org.junit.Assert.*;

public final class TestDI {

	@Test
	public void basic() {
		Module module = Module.create()
				.bind(Integer.class).toInstance(42)
				.bind(String.class).to(i -> "str: " + i, Integer.class);

		Injector injector = Injector.of(module);

		assertEquals("str: 42", injector.getInstance(String.class));
	}

	@Test
	public void singletons() {
		AtomicInteger ref = new AtomicInteger(41);
		Injector injector = Injector.of(Module.create()
				.bind(Integer.class).to(ref::incrementAndGet)
				.bind(String.class).to(i -> "str: " + i, Integer.class));

		assertEquals("str: 42", injector.getInstance(String.class));
		assertEquals("str: 42", injector.getInstance(String.class));
		assertEquals("str: 42", injector.getInstance(String.class));
	}

	@Test
	public void provider() {
		AtomicInteger ref = new AtomicInteger(41);
		Injector injector = Injector.of(Module.create()
				.bind(Integer.class).to(ref::incrementAndGet)
				.bind(String.class).to(i -> "str: " + i.get(), new Key<InstanceProvider<Integer>>() {})
				.bindInstanceProvider(String.class));

		assertEquals("str: 42", injector.getInstance(String.class));
		assertEquals("str: 42", injector.getInstance(String.class));
		assertEquals("str: 42", injector.getInstance(String.class));

		InstanceProvider<String> provider = injector.getInstance(new Key<InstanceProvider<String>>() {});
		assertEquals("str: 42", provider.get());
		assertEquals("str: 42", provider.get());
		assertEquals("str: 42", provider.get());

		assertEquals("str: 42", injector.getInstance(String.class));
		assertEquals(42, injector.getInstance(Integer.class).intValue());
	}

	@Test
	public void eagers() {
		AtomicInteger mut = new AtomicInteger();

		Injector injector = Injector.of(Module.create()
				.bind(String.class).to(() -> "str_" + mut.incrementAndGet()).asEager()
				.bind(Object.class).to(() -> "whatever"));

		injector.createEagerInstances();

		assertEquals("str_1", injector.peekInstance(String.class));
		assertNull(injector.peekInstance(Object.class));

		injector.peekInstance(Float.class);
	}

	@Test
	public void crossmodule() {
		Injector injector = Injector.of(
				Module.create()
						.bind(Integer.class).toInstance(42),
				Module.create()
						.bind(String.class).to(i -> "str: " + i, Integer.class));

		assertEquals("str: 42", injector.getInstance(String.class));
	}

	@Test
	public void overrides() {
		Injector injector = Injector.of(override(
				Module.create()
						.bind(Integer.class).toInstance(17)
						.bind(String.class).to(i -> "str: " + i, Integer.class),
				Module.create()
						.bind(Integer.class).toInstance(42)));

		assertEquals("str: 42", injector.getInstance(String.class));
	}

	@Test
	public void duplicates() {
		Module module = Module.create()
				.bind(Integer.class).toInstance(42)
				.bind(String.class).to(i -> "str1: " + i, Integer.class)
				.bind(String.class).to(i -> "str2: " + i, Integer.class);
		try {
			Injector.of(module);
			fail("should've failed");
		} catch (DIException e) {
			e.printStackTrace();
			assertTrue(e.getMessage().startsWith("Duplicate bindings for key String"));
		}
	}

	@Test
	public void simpleCycle() {
		Module module = Module.create()
				.bind(Integer.class).to($ -> 42, String.class)
				.bind(String.class).to(i -> "str: " + i, Integer.class);

		try {
			Injector.of(module);
			fail("should've failed here");
		} catch (DIException e) {
			e.printStackTrace();
		}
	}

	@Test
	public void advancedCycles() {

		// branch that leads to the cycle(s) (should not be in exception output)
		Module branch = Module.create()
				.bind(Short.class).to(Integer::shortValue, Integer.class)
				.bind(Byte.class).to(Short::byteValue, Short.class);

		Module cyclic1 = Module.create()
				.bind(Integer.class).to($ -> 42, Object.class)
				.bind(Object.class).to($ -> new Object(), String.class)
				.bind(String.class).to(i -> "str: " + i, Float.class)
				.bind(Float.class).to(i -> (float) i, Integer.class);

		try {
			Injector.of(branch, cyclic1);
			fail("should've failed here");
		} catch (DIException e) {
			e.printStackTrace();
		}

		Module cyclic2 = Module.create()
				.bind(Double.class).to($ -> 42.0, Character.class)
				.bind(Character.class).to($ -> 'k', Boolean.class)
				.bind(Boolean.class).to($ -> Boolean.TRUE, Double.class);

		try {
			Injector.of(branch, cyclic1, cyclic2);
			fail("should've failed here");
		} catch (DIException e) {
			e.printStackTrace();
		}
	}

	@Test
	public void dsl() {
		Injector injector = Injector.of(Module.create().scan(new Object() {
			@Provides
			String string(Integer integer) {
				return "str: " + integer;
			}

			@Provides
			Integer integer() {
				return 42;
			}
		}));

		assertEquals("str: 42", injector.getInstance(String.class));
	}

	@Test
	public void namedDsl() {
		Injector injector = Injector.of(Module.create().scan(new Object() {
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
		}));

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

		Injector injector = Injector.of(Module.create()
				.bindInstanceInjector(ClassWithCustomDeps.class)
				.scan(new Object() {

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
				}));

		ClassWithCustomDeps instance = injector.getInstance(ClassWithCustomDeps.class);
		assertNull(instance.string);
		assertNull(instance.raw);
		InstanceInjector<ClassWithCustomDeps> instanceInjector = injector.getInstance(new Key<InstanceInjector<ClassWithCustomDeps>>() {});
		instanceInjector.injectInto(instance);
		assertEquals("str: 42", instance.string);
		assertEquals(42, instance.raw.intValue());
	}

	@Test
	public void inheritedInjects() {
		class ClassWithCustomDeps {

			@Inject
			String string;
		}

		@Inject
		class Inherited extends ClassWithCustomDeps {
		}

		Injector injector = Injector.of(Module.create()
				.bind(TestDI.class).toInstance(TestDI.this) // inherited class has implicit dependency on enclosing class
				.bind(Inherited.class)
				.bind(String.class).to(i -> "str: " + i, Integer.class)
				.bind(Integer.class).toInstance(42));

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
			Injector.of(Module.create().bind(RecursiveA.class));
			fail("should've detected the cycle and fail");
		} catch (DIException e) {
			e.printStackTrace();
		}
	}

	static class RecursiveX {
		@SuppressWarnings({"FieldCanBeLocal", "unused"})
		private final RecursiveY y;

		@Inject
		RecursiveX(RecursiveY y) {this.y = y;}
	}

	static class RecursiveY {
		private final InstanceProvider<RecursiveX> xProvider;

		@Inject
		RecursiveY(InstanceProvider<RecursiveX> provider) {xProvider = provider;}
	}

	@Test
	public void cyclicInjects2() {
		Injector injector = Injector.of(Module.create().bind(RecursiveX.class));

		RecursiveX x = injector.getInstance(RecursiveX.class);
		RecursiveY y = injector.getInstance(RecursiveY.class);
		assertSame(x, y.xProvider.get());
	}

	@Test
	public void optionalInjects() {

		@Inject
		class ClassWithCustomDeps {

			@Inject
			@Optional
			@Nullable
			String string;

			@Inject
			Integer integer;
		}

		Module module = Module.create()
				.bind(TestDI.class).toInstance(TestDI.this)
				.bind(ClassWithCustomDeps.class)
				.bind(Integer.class).toInstance(42);
		Injector injector = Injector.of(module);

		ClassWithCustomDeps instance = injector.getInstance(ClassWithCustomDeps.class);
		assertNull(instance.string);
		assertEquals(42, instance.integer.intValue());

		Injector injector2 = Injector.of(module, Module.create().bind(String.class).to(i -> "str: " + i, Integer.class));

		ClassWithCustomDeps instance2 = injector2.getInstance(ClassWithCustomDeps.class);
		assertEquals("str: 42", instance2.string);
		assertEquals(42, instance2.integer.intValue());

		try {
			Injector injector3 = Injector.of(Module.create()
					.bind(TestDI.class).toInstance(TestDI.this)
					.bind(ClassWithCustomDeps.class)
					.bind(String.class).toInstance("str"));
			injector3.getInstance(ClassWithCustomDeps.class);
			fail("should've failed, but didn't");
		} catch (DIException e) {
			e.printStackTrace();
			assertTrue(e.getMessage().startsWith("Unsatisfied dependencies detected:\n\tkey Integer required"));
		}
	}

	static class MyServiceImpl {
		final String string;
		int value = 0;

		private MyServiceImpl(String string) {
			this.string = string;
		}

		@Inject
		public void setValue(int value) {
			this.value = value;
		}

		@Inject
		static MyServiceImpl create(String string) {
			System.out.println("factory method called once");
			return new MyServiceImpl(string);
		}
	}

	@Test
	public void injectFactoryMethod() {
		Injector injector = Injector.of(Module.create()
				.bind(MyServiceImpl.class)
				.bind(String.class).to(() -> "hello")
				.bind(int.class).toInstance(43));

		MyServiceImpl service = injector.getInstance(MyServiceImpl.class);

		assertEquals("hello", service.string);
		assertEquals(43, service.value);
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
		Injector injector = Injector.of(Module.create()
				.bind(String.class).toInstance("hello")
				.bind(Integer.class).toInstance(42)
				.bind(new Key<Container<Float, String, Integer>>() {}));

		Container<Float, String, Integer> instance = injector.getInstance(new Key<Container<Float, String, Integer>>() {});
		assertEquals("hello", instance.something);
		assertEquals(42, instance.somethingElse.intValue());
	}

	@Test
	public void templatedProvider() {

		class Container<T> {
			private final T object;

			public Container(T object) {
				this.object = object;
			}
		}

		Injector injector = Injector.of(Module.create()
				.bind(new Key<Container<String>>() {})
				.bind(new Key<Container<Integer>>() {})
				.scan(new Object() {

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
				}));

		assertEquals("hello", injector.getInstance(new Key<Container<String>>() {}).object);
		assertEquals(42, injector.getInstance(new Key<Container<Integer>>() {}).object.intValue());
	}

	@Test
	public void optionalProvidesParam() {
		Module module = Module.create().scan(new Object() {
			@Provides
			String string(Integer integer, @io.datakernel.di.annotation.Optional Float f) {
				return "str: " + integer + ", " + f;
			}

			@Provides
			Integer integer() {
				return 42;
			}
		});

		Injector injector = Injector.of(module);
		assertEquals("str: 42, null", injector.getInstance(String.class));

		Injector injector2 = Injector.of(combine(module, Module.create().bind(Float.class).toInstance(3.14f)));
		assertEquals("str: 42, 3.14", injector2.getInstance(String.class));
	}

	@Test
	public void providesIntoSet() {
		Injector injector = Injector.of(Module.create().scan(new Object() {
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

		}));

		Set<String> instance = injector.getInstance(new Key<Set<String>>() {});

		Set<String> expected = Stream.of("str1: 42", "str2: 42", "str3: 42").collect(toSet());

		assertEquals(expected, instance);

		Key<Set<List<String>>> key = new Key<Set<List<String>>>() {};
		Set<List<String>> instance2 = injector.getInstance(key);

		Set<List<String>> expected2 = Stream.of(singletonList("str1: 42"), singletonList("str2: 42")).collect(toSet());

		assertEquals(expected2, instance2);
	}

	@Test
	public void inheritedProviders() {
		class ObjectWithProviders {
			@Provides
			Integer integer() {
				return 123;
			}
		}

		class ObjectWithProviders2 extends ObjectWithProviders {
			@Provides
			String string(Integer integer) {
				return integer.toString();
			}
		}

		Injector injector = Injector.of(Module.create().scan(new ObjectWithProviders2()));
		String string = injector.getInstance(String.class);

		assertEquals("123", string);
	}

	@Test
	public void abstractModuleGenerics() {

		@Inject
		class AndAContainerToo<T> {

			@Inject
			@Named("namedGeneric")
			T object;
		}

		abstract class Module1<D> extends AbstractModule {
			@Provides
			String string(D object) {
				return "str: " + object.toString();
			}
		}
		abstract class Module2<C> extends Module1<C> {
			@Override
			protected void configure() {
				bind(TestDI.class).toInstance(TestDI.this);
				bind(new Key<AndAContainerToo<C>>() {});
			}

			@Provides
			@Named("second")
			String string(List<C> object) {
				return "str: " + object.toString();
			}
		}

		Injector injector = Injector.of(new Module2<Integer>() {
			@Override
			protected void configure() {
				super.configure();
				bind(Integer.class).toInstance(42);
				bind(Integer.class, "namedGeneric").toInstance(-42);
				bind(new Key<List<Integer>>() {}).toInstance(asList(1, 2, 3));
			}
		});

		Utils.printGraphVizGraph(injector.getBindingsTrie());

		assertEquals("str: 42", injector.getInstance(String.class));
		assertEquals("str: [1, 2, 3]", injector.getInstance(Key.of(String.class, "second")));
		assertEquals(-42, injector.getInstance(new Key<AndAContainerToo<Integer>>() {}).object.intValue());
	}

	@Test
	public void injectConstructor() {

		class Injectable {
			String string;
			Integer integer;

			@Inject
			Injectable(String string, @Optional @Nullable Integer integer) {
				this.string = string;
				this.integer = integer;
			}
		}

		Injector injector = Injector.of(Module.create()
				.bind(TestDI.class).toInstance(TestDI.this)
				.bind(Injectable.class)
				.bind(String.class).toInstance("hello"));

		Injectable instance = injector.getInstance(Injectable.class);
		assertEquals("hello", instance.string);
		assertNull(instance.integer);
	}

	@Test
	public void transitiveImplicitBinding() {
		@Inject
		class Container {
			@Inject
			InstanceProvider<InstanceProvider<String>> provider;
		}

		Injector injector = Injector.of(Module.create()
				.bind(TestDI.class).toInstance(TestDI.this)
				.bind(Container.class)
				.bind(String.class).toInstance("hello"));

		Container instance = injector.getInstance(Container.class);

		assertEquals("hello", instance.provider.get().get());
	}

	@Test
	public void mapMultibinding() {

		Key<Map<String, Integer>> key = new Key<Map<String, Integer>>() {};

		Injector injector = Injector.of(Module.create()
				.multibindToMap(String.class, Integer.class)
				.scan(new Object() {

					@Provides
					Integer integer() {
						return 42;
					}

					@Provides
					Map<String, Integer> firstOne() {
						return singletonMap("first", 1);
					}

					@Provides
					Map<String, Integer> second(Integer integer) {
						return singletonMap("second", integer);
					}

					@Provides
					Map<String, Integer> thirdTwo() {
						return singletonMap("third", 2);
					}
				}));

		Map<String, Integer> map = injector.getInstance(key);

		Map<String, Integer> expected = new HashMap<>();
		expected.put("first", 1);
		expected.put("second", 42);
		expected.put("third", 2);

		assertEquals(expected, map);
	}

	@Test
	public void providesNull() {

		Injector injector = Injector.of(Module.create().scan(new Object() {
			@Provides
			Integer integer() {
				return null;
			}

			@Provides
			String string(Integer integer) {
				return "str: " + integer;
			}
		}));

		try {
			injector.getInstance(String.class);
		} catch (DIException e) {
			assertTrue(e.getMessage().startsWith("Binding refused to construct an instance for key Integer"));
		}
	}

	@Retention(RetentionPolicy.RUNTIME)
	@Target(ElementType.METHOD)
	@ScopeAnnotation
	@interface Scope1 {
	}

	@Retention(RetentionPolicy.RUNTIME)
	@Target(ElementType.METHOD)
	@ScopeAnnotation
	@interface Scope2 {
	}

	@Test
	public void ignoringScopes() {
		Module module = Module.create().scan(new Object() {
			@Provides
			@Scope1
			Double first() {
				return 27d;
			}

			@Provides
			@Scope2
			Float second(Double first, Integer top) {
				// static check runs on injector creation so it wont fail
				// (unsatisfied Double from other scope)
				return 34f;
			}

			@Provides
			Integer top() {
				return 42;
			}

			@Provides
			@Scopes({Scope1.class, Scope2.class})
			String deeper(Integer top, Double first) {
				return "deeper";
			}
		});

		printGraphVizGraph(module.getReducedBindingInfo());

		Trie<Scope, Map<Key<?>, BindingInfo>> flattened = Modules.ignoreScopes(module).getReducedBindingInfo();

		printGraphVizGraph(flattened);

		assertEquals(0, flattened.getChildren().size());
		assertEquals(Stream.of(Double.class, Float.class, Integer.class, String.class)
				.map(Key::of)
				.collect(toSet()), flattened.get().keySet());
	}

	@Test
	public void restrictedContainer() {

		class Container<T> {
			final T peer;

			public Container(T object) {
				this.peer = object;
			}
		}

		Injector injector = Injector.of(Module.create()
				.bind(Integer.class).toInstance(42)
				.bind(Float.class).toInstance(34f)
				.bind(Byte.class).toInstance((byte) -1)
				.bind(String.class).toInstance("hello")
				.bind(new Key<Container<Float>>() {})
				.bind(new Key<Container<Byte>>() {})
				.bind(new Key<Container<Integer>>() {})
				.bind(new Key<Container<String>>() {})
				.scan(new Object() {

					@Provides
					<T extends Number> Container<T> provide(T number) {
						System.out.println("called number provider");
						return new Container<>(number);
					}

					@Provides
					<T extends CharSequence> Container<T> provide2(T str) {
						System.out.println("called string provider");
						return new Container<>(str);
					}
				}));

		assertEquals(42, injector.getInstance(new Key<Container<Integer>>() {}).peer.intValue());
		assertEquals("hello", injector.getInstance(new Key<Container<String>>() {}).peer);
	}

	@Test
	public void annotatedTemplate() {

		class Container<T> {
			final T peer;

			public Container(T object) {
				this.peer = object;
			}
		}

		Injector injector = Injector.of(Module.create()
				.bind(String.class).toInstance("hello")
				.bind(new Key<Container<String>>(Name.of("hello")) {})
				.scan(new Object() {

					@Provides
					@Named("hello")
					<T> Container<T> provide(T number) {
						return new Container<>(number);
					}
				}));

		System.out.println(injector.getInstance(new Key<Container<String>>(Name.of("hello")) {}).peer);
	}

	@Test
	public void methodLocalClass() {

		String captured = "captured";

		@Inject
		class MethodLocal {

			MethodLocal() {
			}

			@SuppressWarnings("unused")
			String captured() {
				return captured;
			}
		}
		try {
			Injector.of(Module.create().bind(MethodLocal.class));
			fail("Should've failed here");
		} catch (DIException e) {
			e.printStackTrace();
			assertTrue(e.getMessage().contains("inject annotation on local class that closes over outside variables and/or has no default constructor"));
		}
	}

	@Test
	public void recursiveTemplate() {
		Injector injector = Injector.of(Module.create()
				.bind(new Key<Comparator<String>>() {})
				.bind(new Key<Comparator<Integer>>() {})
				.scan(new Object() {

					@Provides
					<T extends Comparable<? super T>> Comparator<T> naturalComparator() {
						return Comparator.naturalOrder();
					}
				}));

		assertEquals(Comparator.naturalOrder(), injector.getInstance(new Key<Comparator<String>>() {}));
		assertEquals(Comparator.naturalOrder(), injector.getInstance(new Key<Comparator<Integer>>() {}));
	}

	@Test
	public void uninterruptibeBindRequests() {
		Injector injector = Injector.of(Module.create()
				.bind(String.class)
				.bind(String.class)
				.bind(String.class)
				.scan(new Object() {

					@Provides
					String string() {
						return "hello";
					}
				}));

		assertEquals("hello", injector.getInstance(String.class));
	}

	@Test
	public void scopedBindFail() {
		try {
			Injector.of(
					Module.create().bind(String.class).in(Scope1.class),
					Module.create().bind(String.class).toInstance("root string"));
			fail("Should've failed");
		} catch (DIException e) {
			assertTrue(e.getMessage().startsWith("Refused to generate an explicitly requested binding for key String"));
		}
	}

	@Test
	public void scopedBindWin() {
		Injector injector = Injector.of(
				Module.create().bind(String.class).in(Scope1.class),
				Module.create().bind(String.class).in(Scope1.class).toInstance("scoped string"));

		Injector subInjector = injector.enterScope(Scope.of(Scope1.class));

		assertEquals("scoped string", subInjector.getInstance(String.class));
	}

	@Test
	public void basicExports() {
		Injector injector = Injector.of(Module.create()
				.bind(Integer.class).toInstance(3000)
				.bind(String.class).to(i -> "hello #" + i, Integer.class).export());

		printGraphVizGraph(injector.getBindingsTrie());

		assertEquals("hello #3000", injector.getInstance(String.class));
		assertNull(injector.getInstanceOrNull(Integer.class));
	}

	@Test
	public void dslExports() {
		Injector injector = Injector.of(Module.create()
				.scan(new Object() {

					@Provides
					Integer priv() {
						return 3000;
					}

					@Export
					@Provides
					String pub(Integer integer) {
						return "hello #" + integer;
					}
				}));

		printGraphVizGraph(injector.getBindingsTrie());

		assertEquals("hello #3000", injector.getInstance(String.class));
		assertNull(injector.getInstanceOrNull(Integer.class));
	}

	@Test
	public void rebindImportKey() {
		Module importingModule = Module.create()
				.bind(String.class).to(i -> "hello #" + i, Integer.class);

		Module exportingModule = Module.create()
				.bind(Integer.class).named("context-dependent-name").toInstance(3000);

		Injector injector = Injector.of(
				exportingModule,
				importingModule.rebindImport(Key.of(Integer.class), Key.of(Integer.class, "context-dependent-name"))
		);

		assertEquals("hello #3000", injector.getInstance(String.class));
		assertNull(injector.getInstanceOrNull(Integer.class));
		assertEquals(3000, injector.getInstance(Key.of(Integer.class, "context-dependent-name")).intValue());
	}

	@Test
	public void rebindExport() {

		Module exportingModule = Module.create()
				.bind(Integer.class).toInstance(3000);

		Module importingModule = Module.create()
				.bind(String.class).to(i -> "hello #" + i, Key.of(Integer.class, "context-dependent-name"));

		Injector injector = Injector.of(
				exportingModule.rebindExport(Key.of(Integer.class), Key.of(Integer.class, "context-dependent-name")),
				importingModule
		);

		assertEquals("hello #3000", injector.getInstance(String.class));
		assertNull(injector.getInstanceOrNull(Integer.class));
		assertEquals(3000, injector.getInstance(Key.of(Integer.class, "context-dependent-name")).intValue());
	}

	static class MyModule extends AbstractModule {}

	static class InheritedModule extends MyModule {}

	@ShortTypeName("RenamedModule")
	static class OtherModule extends MyModule {}

	@SuppressWarnings("unused")
	static class GenericModule<A, B> extends AbstractModule {}

	@Test
	public void abstractModuleToString() {
		Module module = new AbstractModule() {};
		Module module2 = new MyModule();
		Module module3 = new InheritedModule();
		Module module4 = new GenericModule<String, Integer>() {};
		Module module5 = new OtherModule();

		System.out.println(module2.toString());

		assertTrue(module.toString().startsWith("AbstractModule(at io.datakernel.di.TestDI.abstractModuleToString(TestDI.java:"));
		assertTrue(module2.toString().startsWith("MyModule(at io.datakernel.di.TestDI.abstractModuleToString(TestDI.java:"));
		assertTrue(module3.toString().startsWith("InheritedModule(at io.datakernel.di.TestDI.abstractModuleToString(TestDI.java:"));
		assertTrue(module4.toString().startsWith("GenericModule<String, Integer>(at io.datakernel.di.TestDI.abstractModuleToString(TestDI.java:"));
		assertTrue(module5.toString().startsWith("RenamedModule(at io.datakernel.di.TestDI.abstractModuleToString(TestDI.java:"));
	}

	@Test
	public void changeDisplayName() {

		@ShortTypeName("GreatPojoName")
		class Pojo {}
		class PlainPojo {}

		@SuppressWarnings("unused")
		@ShortTypeName("GreatGenericPojoName")
		class GenericPojo<A, B> {}
		@SuppressWarnings("unused")
		class PlainGenericPojo<A, B> {}

		assertEquals("PlainPojo", Key.of(PlainPojo.class).getDisplayString());
		assertEquals("GreatPojoName", Key.of(Pojo.class).getDisplayString());

		assertEquals("PlainGenericPojo<Integer, List<String>>", new Key<PlainGenericPojo<Integer, List<String>>>() {}.getDisplayString());
		assertEquals("GreatGenericPojoName<Integer, List<String>>", new Key<GenericPojo<Integer, List<String>>>() {}.getDisplayString());
	}

	@Test
	public void exportingMultibinders() {
		Key<Set<String>> setOfStrings = new Key<Set<String>>() {};

		Injector injector = Injector.of(Module.create()
				.bind(setOfStrings).to(() -> singleton("one"))
				.bind(setOfStrings).to(() -> singleton("two"))
				.bind(setOfStrings).to(() -> singleton("three"))
				.multibindToSet(String.class)
				.bind(new Key<List<String>>() {}).to(ArrayList::new, setOfStrings)
				.export(new Key<List<String>>() {}));

		Set<String> expected = new HashSet<>();
		expected.add("one");
		expected.add("two");
		expected.add("three");

		assertEquals(expected, new HashSet<>(injector.getInstance(new Key<List<String>>() {})));
	}

	@Test
	public void rebindImport() {
		Module importingModule = Module.create()
				.bind(String.class).to(i -> "hello #" + i, Integer.class);

		Injector injector = Injector.of(importingModule
				.rebindImport(Key.of(Integer.class), Binding.toInstance(3000)));

		assertEquals("hello #3000", injector.getInstance(String.class));
		assertNull(injector.getInstanceOrNull(Integer.class));
	}

	@Test
	public void recursiveRebindImport() {
		Module importingModule = Module.create()
				.bind(String.class).to(i -> "hello #" + i, Integer.class);

		Module m = Modules.combine(
				Module.create().bind(Integer.class).toInstance(3000),
				importingModule.rebindImport(Key.of(Integer.class), Binding.to(i -> i * 2, Integer.class)));

		printGraphVizGraph(m.getReducedBindingInfo());

		Injector injector = Injector.of(m);

		assertEquals("hello #6000", injector.getInstance(String.class));
		assertEquals(3000, injector.getInstance(Integer.class).intValue());
	}

	@Test
	public void scopedBindImport() {
		Module importingModule = Module.create()
				.bind(String.class).in(Scope1.class).to(i -> "hello #" + i, Integer.class);

		Module m = Modules.combine(
				Module.create().bind(Integer.class).toInstance(1500),
				importingModule.rebindImport(Key.of(Integer.class), Binding.to(i -> i * 2, Integer.class)));

		Injector injector = Injector.of(m);

		Injector subinjector = injector.enterScope(Scope.of(Scope1.class));
		assertEquals("hello #3000", subinjector.getInstance(String.class));
	}

	@Test
	public void exportMultibinders() {
		Module withSet = Module.create()
				.scan(new Object() {

					@ProvidesIntoSet
					String first() {
						return "one";
					}

					@ProvidesIntoSet
					String second() {
						return "two";
					}

					@Export
					@Provides
					Integer integer(Set<String> strings) {
						return strings.size();
					}
				});

		Injector injector = Injector.of(withSet);

		assertEquals(2, injector.getInstance(Integer.class).intValue());
		assertNull(injector.getInstanceOrNull(new Key<Set<String>>() {}));
	}

	@Test
	public void rebindMultibinders() {
		Key<Set<String>> set = new Key<Set<String>>() {};

		Module withSet = Module.create()
				.scan(new Object() {

					@ProvidesIntoSet
					String first(Integer i) {
						return "one #" + i;
					}

					@ProvidesIntoSet
					String second(Integer i) {
						return "two #" + i;
					}
				})
				.rebindImport(Key.of(Integer.class), Key.of(Integer.class, "renamed"));

		Set<String> expected = Stream.of("one #42", "two #42").collect(toSet());

		Injector injector = Injector.of(withSet, Module.create().bind(Key.of(Integer.class, "renamed")).toInstance(42));

		assertEquals(expected, injector.getInstance(set));
		assertNull(injector.getInstanceOrNull(Integer.class));
	}

	@Test
	public void exportTransformers() {
		Module module = Module.create()
				.bind(String.class).to(i -> "str #" + i, Integer.class).export()
				.bind(Integer.class).toInstance(123)
				.transform(1, (bindings, scope, key, binding) -> {
					//noinspection RedundantCast - does not compile without this
					if (key.getRawType() != (Class) Integer.class) {
						return binding;
					}
					return binding.mapInstance(i -> ((Integer) i) * 2);
				});

		Injector injector = Injector.of(module);

		assertEquals("str #246", injector.getInstance(String.class));
		assertNull(injector.getInstanceOrNull(Integer.class));
	}

	@Test
	public void rebindTransformers() {

		Key<Integer> integerKey = Key.of(Integer.class);

		Module module = Module.create()
				.bind(String.class).to(i -> "str #" + i, Integer.class)

				.transform(0, (bindings, scope, key, binding) ->
						key.equals(integerKey) ?
								binding
										.addDependencies(Float.class)
										.mapInstance(singletonList(Key.of(Float.class)), (args, i) -> ((Integer) i) * ((Float) args[0]).intValue()) :
								binding)

				.rebindImport(Key.of(Integer.class), Key.of(Integer.class, "renamed"))
				.rebindImport(Key.of(Float.class), Key.of(Float.class, "renamed"));

		Injector injector = Injector.of(
				module,
				Module.create()
						.bind(Key.of(Integer.class, "renamed")).toInstance(42)
						.bind(Key.of(Float.class, "renamed")).toInstance(2f)
		);

		printGraphVizGraph(injector.getBindingsTrie());

		assertEquals("str #84", injector.getInstance(String.class));

		assertNull(injector.getInstanceOrNull(Integer.class));
	}

	@Test
	public void exportGenerators() {
		Module module = Module.create()
				.bind(String.class).to(i -> "str #" + i, Integer.class).export()
				.bind(Float.class).toInstance(123f)
				.generate(Integer.class, (bindings, scope, key) -> {
					if (scope.length != 0 || !key.equals(Key.of(Integer.class))) {
						return null;
					}
					return Binding.to(Float::intValue, Float.class);
				});

		Injector injector = Injector.of(module);

		assertEquals("str #123", injector.getInstance(String.class));
		assertNull(injector.getInstanceOrNull(Float.class));
	}

	@Test
	public void rebindGenerators() {
		Module module = Module.create()
				.bind(String.class).to(i -> "str #" + i, Integer.class)
				.generate(Integer.class, (bindings, scope, key) -> {
					if (scope.length != 0 || !key.equals(Key.of(Integer.class))) {
						return null;
					}
					return Binding.to(Float::intValue, Float.class);
				})
				.rebindImport(Key.of(Float.class), Key.of(Float.class, "renamed"));

		Injector injector = Injector.of(
				module,
				Module.create()
						.bind(Key.of(Float.class, "renamed")).toInstance(42f));

		assertEquals("str #42", injector.getInstance(String.class));
	}

	@Test
	public void moduleInstallReexport() {
		Injector injector = Injector.of(
				Module.create()
						.bind(String.class).toInstance("hello").export()
						.bind(Integer.class).export()
						.install(Module.create()
								.bind(Integer.class).toInstance(123)));

		assertEquals("hello", injector.getInstance(String.class));
		assertEquals(123, injector.getInstance(Integer.class).intValue());
	}

	public interface TestInterface<T> {
		T getObj();
	}

	public static class StringInterface implements TestInterface<String> {
		static int counter = 0;

		private final String obj;

		public StringInterface(String obj) {
			this.obj = obj;
			counter++;
		}

		@Override
		public String getObj() {
			return obj;
		}
	}

	@Test
	public void bindIntoSetBug() {
		Injector injector = Injector.of(
				new AbstractModule() {
					@Override
					protected void configure() {
						bindIntoSet(new Key<TestInterface<?>>() {}, Key.of(StringInterface.class));
					}

					@Provides
					StringInterface testBindIntoSet() {
						return new StringInterface("string");
					}
				});
		Set<TestInterface<?>> interfaces = injector.getInstance(new Key<Set<TestInterface<?>>>() {});

		injector.getInstance(StringInterface.class);

		assertEquals(1, interfaces.size());
		assertEquals(1, StringInterface.counter); // bug: 1 != 2
	}

	public interface PostConstruct {
		void init();
	}

	public static class PostConstructModule extends AbstractModule {
		@Override
		protected void configure() {
			transform(99, (bindings, scope, key, binding) -> {
				if (PostConstruct.class.isAssignableFrom(key.getRawType())) {
					return binding.mapInstance(obj -> {
						((PostConstruct) obj).init();
						return obj;
					});
				}
				return binding;
			});
		}
	}

	public static class PostConstructed implements PostConstruct {
		private final String s;
		private boolean inited;

		public PostConstructed(String s) {
			this.s = s;
		}

		@Override
		public void init() {
			inited = true;
		}

		@Override
		public String toString() {
			return s;
		}
	}

	@Test
	public void transientMap() {
		AtomicInteger mut = new AtomicInteger();
		Injector injector = Injector.of(Module.create()
						.bind(PostConstructed.class).to(() -> new PostConstructed("str_" + mut.incrementAndGet())).asTransient(),
				new PostConstructModule());

		PostConstructed instance1 = injector.getInstance(PostConstructed.class);
		assertEquals("str_1", instance1.s);
		assertTrue(instance1.inited);
		PostConstructed instance2 = injector.getInstance(PostConstructed.class);
		assertEquals("str_2", instance2.s);
		assertTrue(instance2.inited);
		PostConstructed instance3 = injector.getInstance(PostConstructed.class);
		assertEquals("str_3", instance3.s);
		assertTrue(instance3.inited);
	}

	@Test
	public void transientBinding() {
		AtomicInteger mut = new AtomicInteger();
		Injector injector = Injector.of(Module.create()
				.bind(Integer.class).to(mut::incrementAndGet).asTransient()
				.bind(String.class, "fixed").to(i -> "str_" + i, Integer.class)
				.bind(String.class).to(i -> "str_" + i, Integer.class).asTransient());

		assertEquals(5, Stream.generate(() -> injector.getInstance(Integer.class)).limit(5).collect(toSet()).size());
		assertEquals(1, Stream.generate(() -> injector.getInstance(Key.of(String.class, "fixed"))).limit(5).collect(toSet()).size());
		assertEquals(5, Stream.generate(() -> injector.getInstance(String.class)).limit(5).collect(toSet()).size());
	}

	@Test
	public void transientDsl() {
		AtomicInteger counter = new AtomicInteger();
		AtomicInteger mut = new AtomicInteger();

		Injector injector = Injector.of(Module.create()
				.bind(String.class).to(() -> {
					counter.incrementAndGet();
					return "str_";
				})
				.bind(Integer.class).to(mut::incrementAndGet).asTransient()
				.scan(new Object() {

					@Provides
					@Named("t")
					@Transient
					String string(String s, Integer i) {
						return s + i;
					}

					@Provides
					@Named("nt")
					String string2(String s, Integer i) {
						return s + i;
					}
				}));

		assertEquals(5, Stream.generate(() -> injector.getInstance(Key.of(String.class, "t"))).limit(5).collect(toSet()).size());
		assertEquals(1, Stream.generate(() -> injector.getInstance(Key.of(String.class, "nt"))).limit(5).collect(toSet()).size());
		assertEquals(7, injector.getInstance(Integer.class).intValue());
		assertEquals(1, counter.get());
	}

	@Test
	public void transientGenerators() {
		Key<List<String>> stringListKey = new Key<List<String>>() {};
		Key<Set<String>> stringSetKey = new Key<Set<String>>() {};
		AtomicInteger mut = new AtomicInteger();

		Injector injector = Injector.of(Module.create()

				.bind(stringListKey).asTransient()
				.bind(stringSetKey)

				.bind(String.class).asTransient()

				.generate(String.class, (bindings, scope, key) ->
						Binding.to(() -> "str_" + mut.incrementAndGet()))

				.scan(new Object() {
					@Provides
					<T> List<T> transientList(T t) {
						return singletonList(t);
					}

					@Provides
					<T> Set<T> notSoTransientSet(T t) {
						return singleton(t);
					}
				}));

		assertEquals(5, Stream.generate(() -> injector.getInstance(stringListKey)).limit(5).collect(toSet()).size());
		assertEquals(1, Stream.generate(() -> injector.getInstance(stringSetKey)).limit(5).collect(toSet()).size());
	}

	@Test
	public void transientPlainBind() {
		AtomicInteger mut = new AtomicInteger();

		Injector injector = Injector.of(Module.create()
				.bind(String.class).asTransient()
				.bind(String.class, "nt")
				.generate(String.class, (bindings, scope, key) ->
						Binding.to(() -> "str_" + mut.incrementAndGet())));

		assertEquals(5, Stream.generate(() -> injector.getInstance(String.class)).limit(5).collect(toSet()).size());
		assertEquals(1, Stream.generate(() -> injector.getInstance(Key.of(String.class, "nt"))).limit(5).collect(toSet()).size());
	}

	@Test
	public void partiallyTransientMultibind() {
		AtomicInteger mut = new AtomicInteger();
		Constructor0<Set<String>> constructor = () -> singleton("str_" + mut.incrementAndGet());

		Key<Set<String>> setKey = new Key<Set<String>>() {};
		Key<Set<String>> setKeyNt = setKey.named("nt");

		Injector injector = Injector.of(Module.create()
				.bind(setKey).to(constructor).asTransient()
				.bind(setKey).toInstance(singleton("other one"))

				.bind(setKeyNt).to(constructor)
				.bind(setKeyNt).toInstance(singleton("other one"))

				.bind(new Key<Set<String>>() {}).asTransient()

				.multibindToSet(String.class)
				.multibindToSet(Key.of(String.class, "nt")));

		assertEquals(5, Stream.generate(() -> injector.getInstance(setKey)).limit(5).collect(toSet()).size());
		assertEquals(1, Stream.generate(() -> injector.getInstance(setKeyNt)).limit(5).collect(toSet()).size());
	}

	@Test
	public void plainBindPeekInstance() {
		Injector injector = Injector.of(Module.create()
				.bind(PostConstructed.class).to(() -> new PostConstructed("hello world"))
				.bind(PostConstruct.class).to(PostConstructed.class));

		PostConstruct instance = injector.getInstance(PostConstruct.class);
		assertEquals(instance, injector.peekInstance(PostConstruct.class));
	}
}
