package io.datakernel.di;

import io.datakernel.di.annotation.Optional;
import io.datakernel.di.annotation.*;
import io.datakernel.di.core.*;
import io.datakernel.di.module.AbstractModule;
import io.datakernel.di.module.Module;
import io.datakernel.di.module.Modules;
import io.datakernel.di.util.ReflectionUtils;
import io.datakernel.di.util.Trie;
import org.jetbrains.annotations.Nullable;
import org.junit.Test;

import java.lang.annotation.*;
import java.util.*;
import java.util.stream.Stream;

import static io.datakernel.di.module.Modules.combine;
import static io.datakernel.di.module.Modules.override;
import static io.datakernel.di.util.Types.parameterized;
import static io.datakernel.di.util.Utils.printGraphVizGraph;
import static java.util.Arrays.asList;
import static java.util.Collections.*;
import static java.util.stream.Collectors.toSet;
import static org.junit.Assert.*;

public final class TestDI {

	@Test
	public void basic() {
		AbstractModule module = new AbstractModule() {{
			bind(Integer.class).toInstance(42);
			bind(String.class).to(i -> "str: " + i, Integer.class);
		}};

		Injector injector = Injector.of(module);

		assertEquals("str: 42", injector.getInstance(String.class));
	}

	@Test
	public void singletons() {
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
			bind(String.class).to(i -> "str: " + i.create(), new Key<InstanceFactory<Integer>>() {});
			bind(new Key<InstanceFactory<String>>() {});
		}});

		assertEquals("str: 42", injector.getInstance(String.class));
		assertEquals("str: 42", injector.getInstance(String.class));
		assertEquals("str: 42", injector.getInstance(String.class));

		InstanceFactory<String> provider = injector.getInstance(new Key<InstanceFactory<String>>() {});
		assertEquals("str: 43", provider.create());
		assertEquals("str: 44", provider.create());
		assertEquals("str: 45", provider.create());

		// the first getInstance call was cached, non-pure mutability affects results only when using .create
		assertEquals("str: 42", injector.getInstance(String.class));

		// but the integer getInstance or even provideSingleton were never called and so it was never cached
		// mutable factory is only to indicate multiple factory calls in testing, in reality nobody
		// should really do non-pure factories unless they know what hack they're trying to accomplish
		assertEquals(46, injector.getInstance(Integer.class).intValue());
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

	@Test
	public void duplicates() {
		Module module = new AbstractModule() {{
			bind(Integer.class).toInstance(42);
			bind(String.class).to(i -> "str1: " + i, Integer.class);
			bind(String.class).to(i -> "str2: " + i, Integer.class);
		}};
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
		AbstractModule module = new AbstractModule() {{
			bind(Integer.class).to($ -> 42, String.class);
			bind(String.class).to(i -> "str: " + i, Integer.class);
		}};

		try {
			Injector.of(module);
			fail("should've failed here");
		} catch (DIException e) {
			e.printStackTrace();
		}
	}

	@Test
	public void advancedCycles() {

		Module branch = new AbstractModule() {{
			// branch that leads to the cycle(s) (should not be in exception output)
			bind(Short.class).to(Integer::shortValue, Integer.class);
			bind(Byte.class).to(Short::byteValue, Short.class);
		}};

		Module cyclic1 = new AbstractModule() {{
			bind(Integer.class).to($ -> 42, Object.class);
			bind(Object.class).to($ -> new Object(), String.class);
			bind(String.class).to(i -> "str: " + i, Float.class);
			bind(Float.class).to(i -> (float) i, Integer.class);
		}};

		Set<Key<?>> expected1 = cyclic1.resolveBindings().get().keySet();

		try {
			Injector.of(branch, cyclic1);
			fail("should've failed here");
		} catch (DIException e) {
			e.printStackTrace();

			Set<Key<?>[]> cycles = BindingGraph.getCyclicDependencies(combine(branch, cyclic1).resolveBindings());
			assertEquals(1, cycles.size());
			assertEquals(expected1, Arrays.stream(cycles.iterator().next()).collect(toSet()));
		}

		Module cyclic2 = new AbstractModule() {{
			bind(Double.class).to($ -> 42.0, Character.class);
			bind(Character.class).to($ -> 'k', Boolean.class);
			bind(Boolean.class).to($ -> Boolean.TRUE, Double.class);
		}};

		Set<Key<?>> expected2 = cyclic2.resolveBindings().get().keySet();

		try {
			Injector.of(branch, cyclic1, cyclic2);
			fail("should've failed here");
		} catch (DIException e) {
			e.printStackTrace();

			Set<Key<?>[]> cycles = BindingGraph.getCyclicDependencies(combine(branch, cyclic1, cyclic2).resolveBindings());
			assertEquals(2, cycles.size());

			Set<Set<Key<?>>> unorderedCycles = cycles.stream().map(cycle -> Arrays.stream(cycle).collect(toSet())).collect(toSet());
			assertEquals(Stream.of(expected1, expected2).collect(toSet()), unorderedCycles);
		}
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
			@Override
			protected void configure() {
				bind(new Key<InstanceInjector<ClassWithCustomDeps>>() {});
			}

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

		Injector injector = Injector.of(new AbstractModule() {{
			bind(TestDI.class).toInstance(TestDI.this); // inherited class has implicit dependency on enclosing class
			bind(Inherited.class);
			bind(String.class).to(i -> "str: " + i, Integer.class);
			bind(Integer.class).toInstance(42);
		}});

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
				bind(RecursiveA.class);
			}};

			Injector.of(module);
			fail("should've detected the cycle and fail");
		} catch (DIException e) {
			e.printStackTrace();
		}
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

		Module module = new AbstractModule() {{
			bind(TestDI.class).toInstance(TestDI.this);
			bind(ClassWithCustomDeps.class);
			bind(Integer.class).toInstance(42);
		}};

		Injector injector = Injector.of(module);

		ClassWithCustomDeps instance = injector.getInstance(ClassWithCustomDeps.class);
		assertNull(instance.string);
		assertEquals(42, instance.integer.intValue());

		Injector injector2 = Injector.of(module, new AbstractModule() {{
			bind(String.class).to(i -> "str: " + i, Integer.class);
		}});

		ClassWithCustomDeps instance2 = injector2.getInstance(ClassWithCustomDeps.class);
		assertEquals("str: 42", instance2.string);
		assertEquals(42, instance2.integer.intValue());

		try {
			Injector injector3 = Injector.of(new AbstractModule() {{
				bind(TestDI.class).toInstance(TestDI.this);
				bind(ClassWithCustomDeps.class);
				bind(String.class).toInstance("str");
			}});
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

		Injector injector = Injector.of(new AbstractModule() {{
			bind(MyServiceImpl.class);
			bind(String.class).to(() -> "hello");
			bind(int.class).toInstance(43);
		}});

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

		AbstractModule module = new AbstractModule() {{
			bind(String.class).toInstance("hello");
			bind(Integer.class).toInstance(42);
			bind(new Key<Container<Float, String, Integer>>() {});
		}};

		Injector injector = Injector.of(module);

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

		AbstractModule module = new AbstractModule() {

			@Override
			protected void configure() {
				bind(new Key<Container<String>>() {});
				bind(new Key<Container<Integer>>() {});
			}

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
	public void optionalProvidesParam() {
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
	public void injeritedProviders() {
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
	public void genericModules() {

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

		Injector injector = Injector.of(new Module2<Integer>() {{
			bind(Integer.class).toInstance(42);
			bind(Key.of(Integer.class, "namedGeneric")).toInstance(-42);
			bind(new Key<List<Integer>>() {}).toInstance(asList(1, 2, 3));
		}});

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

		Injector injector = Injector.of(new AbstractModule() {{
			bind(TestDI.class).toInstance(TestDI.this);
			bind(Injectable.class);
			bind(String.class).toInstance("hello");
		}});

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

		Injector injector = Injector.of(new AbstractModule() {{
			bind(TestDI.class).toInstance(TestDI.this);
			bind(Container.class);
			bind(String.class).toInstance("hello");
		}});

		Container instance = injector.getInstance(Container.class);

		assertEquals("hello", instance.provider.get().get());
	}

	@Test
	public void mapMultibinding() {

		Key<Map<String, Integer>> key = new Key<Map<String, Integer>>() {};

		Injector injector = Injector.of(new AbstractModule() {

			@Override
			protected void configure() {
				multibind(key, Multibinder.toMap());
			}

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
		});

		Map<String, Integer> map = injector.getInstance(key);

		Map<String, Integer> expected = new HashMap<>();
		expected.put("first", 1);
		expected.put("second", 42);
		expected.put("third", 2);

		assertEquals(expected, map);
	}

	@Test
	public void providesNull() {

		Injector injector = Injector.of(new AbstractModule() {

			@Provides
			Integer integer() {
				return null;
			}

			@Provides
			String string(Integer integer) {
				return "str: " + integer;
			}
		});

		try {
			injector.getInstance(String.class);
		} catch (DIException e) {
			assertTrue(e.getMessage().startsWith("Binding refused to construct an instance for key Integer"));
		}
	}

	@Target(ElementType.METHOD)
	@Retention(RetentionPolicy.RUNTIME)
	@KeySetAnnotation
	@interface MyKeySet {
	}

	@Target(ElementType.METHOD)
	@Retention(RetentionPolicy.RUNTIME)
	@KeySetAnnotation
	@interface MyKeySet2 {
	}

	@Test
	public void keySets() {

		Injector injector = Injector.of(new AbstractModule() {

			@Override
			protected void configure() {
				bind(Integer.class).annotatedWith(Name.of("test")).toInstance(123).as(MyKeySet2.class);
				bind(String.class).annotatedWith(Name.of("test")).toInstance("123").as(MyKeySet2.class);
			}

			@Provides
			@MyKeySet
			Integer integer() {
				return 42;
			}

			@Provides
			@MyKeySet
			Float f() {
				return 42f;
			}

			@Provides
			@MyKeySet
			Double d() {
				return 42d;
			}

			@Provides
			String string(Integer integer) {
				return "str: " + integer;
			}
		});

		Set<Key<?>> keys = injector.getInstance(new Key<Set<Key<?>>>(Name.of(MyKeySet.class)) {});
		assertEquals(Stream.of(Float.class, Double.class, Integer.class).map(Key::of).collect(toSet()), keys);

		Set<Key<?>> keys2 = injector.getInstance(new Key<Set<Key<?>>>(Name.of(MyKeySet2.class)) {});

		assertEquals(Stream.of(String.class, Integer.class).map(cls -> Key.of(cls, "test")).collect(toSet()), keys2);
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
		Module module = new AbstractModule() {

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
		};

		printGraphVizGraph(module.resolveBindings());

		Trie<Scope, Map<Key<?>, Binding<?>>> flattened = Modules.ignoreScopes(module).resolveBindings();

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
				peer = object;
			}
		}

		Injector injector = Injector.of(new AbstractModule() {

			@Override
			protected void configure() {
				bind(Integer.class).toInstance(42);
				bind(Float.class).toInstance(34f);
				bind(Byte.class).toInstance((byte) -1);

				bind(String.class).toInstance("hello");

				bind(new Key<Container<Float>>() {});
				bind(new Key<Container<Byte>>() {});
				bind(new Key<Container<Integer>>() {});

				bind(new Key<Container<String>>() {});
			}

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
		});

		assertEquals(42, injector.getInstance(new Key<Container<Integer>>() {}).peer.intValue());
		assertEquals("hello", injector.getInstance(new Key<Container<String>>() {}).peer);
	}

	@Test
	public void annotatedTemplate() {

		class Container<T> {
			final T peer;

			public Container(T object) {
				peer = object;
			}
		}

		Injector injector = Injector.of(new AbstractModule() {

			@Override
			protected void configure() {
				bind(String.class).toInstance("hello");
				bind(new Key<Container<String>>(Name.of("hello")) {});
			}

			@Provides
			@Named("hello") <T> Container<T> provide(T number) {
				return new Container<>(number);
			}
		});

		System.out.println(injector.getInstance(new Key<Container<String>>(Name.of("hello")) {}).peer);
	}

	@Test
	@SuppressWarnings("AssertEqualsBetweenInconvertibleTypes")
	public void keyEquality() {
		assertEquals(new Key<String>() {}, Key.of(String.class));
		assertNotEquals(new Key<String>() {}, Key.of(Integer.class));

		assertEquals(new Key<List<String>>() {}, Key.ofType(parameterized(List.class, String.class)));
		assertNotEquals(new Key<List<String>>() {}, Key.ofType(parameterized(List.class, Integer.class)));

		assertEquals(new Key<List<String>>("hello") {}, Key.ofType(parameterized(List.class, String.class), "hello"));
		assertNotEquals(new Key<List<String>>("hello") {}, Key.ofType(parameterized(List.class, String.class), "goodbye"));

		java8reflectionBugWorkaround();
	}

	// test the annotated type args for java 9
	// needs to be in static method because of java 8 reflection bugs
	@SuppressWarnings("AssertEqualsBetweenInconvertibleTypes")
	public static void java8reflectionBugWorkaround() {
		assertEquals(new Key<@Named("hello") String>() {}, Key.of(String.class, "hello"));
		assertNotEquals(new Key<@Named("greetings") String>() {}, Key.of(String.class, "hello"));

		assertEquals(new Key<@Named("hello") String>() {}, new Key<String>("hello") {});
		assertNotEquals(new Key<@Named("greetings") String>() {}, new Key<String>("hello") {});

		// nested annotations are ignored
		assertEquals(new Key<List<@Named("hello") String>>() {}, new Key<List<String>>() {});
		assertEquals(new Key<List<@Named("greetings") String>>() {}, new Key<List<String>>() {});
	}

	@Retention(RetentionPolicy.RUNTIME)
	@Target(ElementType.METHOD)
	@interface TestAnnotation {
		String value() default "default";

		int secondParam();
	}

	@Test
	public void annotationProxy() {
		Map<String, Object> args = new HashMap<>();
		args.put("value", "hello");
		args.put("secondParam", 42);

		TestAnnotation proxied = ReflectionUtils.createAnnotationInstance(TestAnnotation.class, args);

		assertEquals("hello", proxied.value());
		assertEquals(42, proxied.secondParam());

		try {
			ReflectionUtils.createAnnotationInstance(TestAnnotation.class, singletonMap("secondParam", 42));
			fail("Should've failed");
		} catch (IllegalArgumentException e) {
			e.printStackTrace();
		}
	}

	@Test
	@EagerSingleton
	public void statelessAnnotationEquality() throws NoSuchMethodException {
		EagerSingleton dummyMarker = ReflectionUtils.createAnnotationInstance(EagerSingleton.class, emptyMap());
		EagerSingleton realMarker = getClass().getMethod("statelessAnnotationEquality").getAnnotation(EagerSingleton.class);

		assertEquals(dummyMarker, realMarker);
		assertEquals(realMarker, dummyMarker);
	}

	@Test
	@Named("hello world")
	public void statefulAnnotationEquality() throws NoSuchMethodException {
		Named proxied = ReflectionUtils.createAnnotationInstance(Named.class, singletonMap("value", "hello world"));
		Named real = getClass().getMethod("statefulAnnotationEquality").getAnnotation(Named.class);

		assertEquals(proxied, real);
		assertEquals(real, proxied);

		Annotation proxied2 = ReflectionUtils.createAnnotationInstance(Named.class, singletonMap("value", "goodbye world"));
		assertNotEquals(proxied2, real);
		assertNotEquals(real, proxied2);
	}
}
