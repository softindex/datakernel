package io.datakernel.di.util;

import io.datakernel.di.*;
import io.datakernel.di.module.AbstractModule;
import io.datakernel.di.module.Module;
import io.datakernel.di.module.Provides;
import io.datakernel.di.util.Constructors.Factory;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.lang.annotation.Annotation;
import java.lang.reflect.*;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static io.datakernel.di.util.Utils.checkArgument;
import static java.util.stream.Collectors.toList;
import static java.util.stream.Collectors.toSet;

public final class ReflectionUtils {
	private ReflectionUtils() {
		throw new AssertionError("nope.");
	}

	private static <T> Key<T> keyOf(@NotNull Type type, Annotation[] annotations) {
		Set<Annotation> names = Arrays.stream(annotations)
				.filter(annotation -> annotation.annotationType().getAnnotation(NameAnnotation.class) != null)
				.collect(toSet());
		if (names.size() > 1) {
			throw new RuntimeException("more than one name annotation");
		}
		return names.isEmpty() ?
				Key.ofType(type) :
				Key.ofType(type, names.iterator().next());
	}

	private static Scope[] scopesFrom(Annotation[] annotations) {
		Set<Annotation> scopes = Arrays.stream(annotations)
				.filter(annotation -> annotation.annotationType().getAnnotation(ScopeAnnotation.class) != null)
				.collect(toSet());

		Scopes nested = (Scopes) Arrays.stream(annotations).filter(annotation -> annotation.annotationType() == Scopes.class)
				.findAny()
				.orElse(null);

		if (scopes.size() > 1) {
			throw new RuntimeException("more than one scope annotation");
		}
		if (!scopes.isEmpty() && nested != null) {
			throw new RuntimeException("cannot have both @Scoped and other scope annotations");
		}
		return nested != null ?
				Arrays.stream(nested.value()).map(Scope::of).toArray(Scope[]::new) :
				scopes.isEmpty() ?
						new Scope[0] :
						new Scope[]{Scope.of(scopes.iterator().next())};
	}

	public static Trie<Scope, Map<Key<?>, Set<Binding<?>>>> getDeclatativeBindings(Object module) {
		Trie<Scope, Map<Key<?>, Set<Binding<?>>>> bindings = Trie.leaf(new HashMap<>());

		for (Method method : module.getClass().getDeclaredMethods()) {
			if (!method.isAnnotationPresent(Provides.class)) {
				continue;
			}
			Annotation[] annotations = method.getDeclaredAnnotations();
			Key<Object> key = keyOf(method.getGenericReturnType(), annotations);
			bindings.computeIfAbsent(scopesFrom(annotations), $ -> new HashMap<>())
					.get()
					.computeIfAbsent(key, $ -> new HashSet<>())
					.add(bindingForMethod(module, method).apply(injectingInitializer(key)));
		}
		return bindings;
	}

	private static Field[] getInjectableFields(Class<?> cls) {
		List<Field> fields = new ArrayList<>();
		while (cls != null) {
			for (Field field : cls.getDeclaredFields()) {
				if (field.getAnnotation(Inject.class) != null) {
					field.setAccessible(true);
					fields.add(field);
				}
			}
			cls = cls.getSuperclass();
		}
		return fields.toArray(new Field[0]);
	}

	public static <T> BindingInitializer<T> injectingInitializer(Key<T> returnType) {
		Field[] injectableFields = getInjectableFields(returnType.getRawType());

		Type[] typeParameters = returnType.getRawType().getTypeParameters();
		Type[] typeArguments = returnType.getTypeParams();
		if (typeParameters.length != typeArguments.length) {
			throw new RuntimeException(returnType.getType() + " has number of type parameters not equal to number of type arguments");
		}

		Map<Type, Type> typevars = IntStream.range(0, typeParameters.length)
				.boxed()
				.collect(Collectors.toMap(i -> typeParameters[i], i -> typeArguments[i]));

		Dependency[] dependencies = Arrays.stream(injectableFields)
				.map(field -> {
					Type type = field.getGenericType();
					Key<Object> key = keyOf(typevars.getOrDefault(type, type), field.getDeclaredAnnotations());
					boolean required = !field.getAnnotation(Inject.class).optional();
					return new Dependency(key, required);
				})
				.toArray(Dependency[]::new);

		return BindingInitializer.of(dependencies, (instance, args) -> {
			for (int i = 0; i < injectableFields.length; i++) {
				Object arg = args[i];
				if (arg != null) {
					try {
						injectableFields[i].set(instance, arg);
					} catch (IllegalAccessException e) {
						throw new RuntimeException("failed to inject field " + injectableFields[i], e);
					}
				}
			}
		});
	}

	@NotNull
	private static Dependency[] toDependencies(Parameter[] parameters) {
		return Arrays.stream(parameters)
				.map(parameter -> new Dependency(keyOf(parameter.getParameterizedType(), parameter.getDeclaredAnnotations()), true))
				.toArray(Dependency[]::new);
	}

	@SuppressWarnings("unchecked")
	private static <T> Binding<T> bindingForMethod(@Nullable Object module, Method method) {
		method.setAccessible(true);

		Key<T> returnType = Key.ofType(method.getGenericReturnType());
		Dependency[] dependencies = toDependencies(method.getParameters());

		return Binding.of(dependencies, args -> {
			try {
				return (T) method.invoke(module, args);
			} catch (IllegalAccessException | InvocationTargetException e) {
				throw new RuntimeException("failed to call method for " + returnType, e);
			}
		}, new LocationInfo(module != null ? module.getClass() : null, method.toString()));
	}

	private static <T> Binding<T> bindingForConstructor(Key<T> key, Constructor<T> constructor) {
		constructor.setAccessible(true);

		Dependency[] dependencies = toDependencies(constructor.getParameters());

		return Binding.of(dependencies, args -> {
			try {
				return constructor.newInstance(args);
			} catch (InstantiationException | IllegalAccessException | InvocationTargetException e) {
				throw new RuntimeException("failed to create " + key, e);
			}
		}, new LocationInfo(null, constructor.toString()));
	}

	@SuppressWarnings("unchecked")
	@Nullable
	public static <T> Binding<T> generateImplicitBinding(Key<T> key) {
		Class<?> cls = key.getRawType();

		if (cls == Provider.class) {
			Type[] typeParams = key.getTypeParams();
			checkArgument(typeParams.length == 1);

			return (Binding<T>) Binding.of(new Key[]{Key.of(Injector.class)}, args -> {
				Injector injector = (Injector) args[0];
				Injector current = injector;
				Binding<?> binding = null;
				while (current != null && binding == null) {
					binding = current.getBinding(Key.ofType(typeParams[0], key.getName()));
					current = current.getParent();
				}
				if (binding == null) {
					return null;
				}
				Factory<?> factory = binding.getFactory();
				Object[] depInstances = Arrays.stream(binding.getDependencies())
						.map(dependency -> dependency.isRequired() ?
								injector.getInstance(dependency.getKey()) :
								injector.getOptionalInstance(dependency.getKey()).orElse(null))
						.toArray(Object[]::new);

				return new Provider<Object>() {
					@Override
					public Object provideNew() {
						return factory.create(depInstances);
					}

					@Override
					public synchronized Object provideSingleton() {
						return injector.getInstance(key);
					}
				};
			});
		}

		Inject classInjectAnnotation = cls.getAnnotation(Inject.class);

		if (classInjectAnnotation != null) {
			if (classInjectAnnotation.optional()) {
				throw new RuntimeException("inject annotation on class cannot be optional");
			}
			try {
				return bindingForConstructor(key, (Constructor<T>) cls.getDeclaredConstructor());
			} catch (NoSuchMethodException e) {
				throw new RuntimeException("inject annotation on class with no default constructor", e);
			}
		} else {
			Set<Constructor<?>> injectConstructors = Arrays.stream(cls.getDeclaredConstructors())
					.filter(c -> c.getAnnotation(Inject.class) != null)
					.collect(toSet());

			if (injectConstructors.size() > 1) {
				throw new RuntimeException("more than one inject constructor");
			}
			if (!injectConstructors.isEmpty()) {
				return bindingForConstructor(key, (Constructor<T>) injectConstructors.iterator().next());
			}
		}

		Set<Method> factoryMethods = Arrays.stream(cls.getDeclaredMethods())
				.filter(method -> method.getAnnotation(Inject.class) != null)
				.collect(toSet());

		boolean allOk = factoryMethods.stream()
				.allMatch(method -> method.getReturnType() == cls
						&& Modifier.isPublic(method.getModifiers())
						&& Modifier.isStatic(method.getModifiers()));

		if (!allOk) {
			throw new RuntimeException("found methods with @Inject annotation that are not public static factory methods for key " + key);
		}
		if (factoryMethods.size() > 1) {
			throw new RuntimeException("more than one inject factory method");
		}
		if (!factoryMethods.isEmpty()) {
			return bindingForMethod(null, factoryMethods.iterator().next());
		}
		return null;
	}

	public static void addImplicitBindings(Trie<Scope, Map<Key<?>, Binding<?>>> bindings) {
		addImplicitBindings(new HashSet<>(), bindings);
	}

	@SuppressWarnings("unchecked")
	private static void addImplicitBindings(Set<Key<?>> known, Trie<Scope, Map<Key<?>, Binding<?>>> bindings) {
		Map<Key<?>, Binding<?>> localBindings = bindings.get();
		known.addAll(localBindings.keySet());
		List<Binding<?>> bindingsToCheck = new ArrayList<>(localBindings.values());
		do {
			bindingsToCheck = bindingsToCheck.stream()
					.flatMap(binding -> Arrays.stream(binding.getDependencies()))
					.filter(dependency -> !known.contains(dependency.getKey()))
					.map(dependency -> {
						Key<Object> key = (Key<Object>) dependency.getKey();
						Binding<Object> binding = generateImplicitBinding(key);
						if (binding != null) {
							known.add(key);
							localBindings.put(key, binding.apply(injectingInitializer(key)));
						} else if (dependency.isRequired()) {
							throw new RuntimeException("unsatisfied dependency " + key + " with no implicit bindings");
						}
						return binding;
					})
					.filter(Objects::nonNull)
					.collect(toList());
		} while (!bindingsToCheck.isEmpty());

		bindings.getChildren().values().forEach(sub -> addImplicitBindings(known, sub));
	}

	// throws on unsatisfied dependencies, returns list of cycles
	// this is a subject to change ofc
	public static Set<Key<?>[]> getCycles(Trie<Scope, Map<Key<?>, Binding<?>>> bindings) {
		return getCycles(new HashSet<>(), bindings).collect(toSet());
	}

	private static Stream<Key<?>[]> getCycles(Set<Key<?>> visited, Trie<Scope, Map<Key<?>, Binding<?>>> bindings) {
		Map<Key<?>, Binding<?>> localBindings = bindings.get();
		LinkedHashSet<Key<?>> visiting = new LinkedHashSet<>();
		Set<Key<?>[]> cycles = new HashSet<>();
		localBindings.forEach((key, binding) -> dfs(localBindings, visited, visiting, cycles, new Dependency(key, true)));

		return Stream.concat(
				cycles.stream(),
				bindings.getChildren().values().stream().flatMap(sub -> getCycles(new HashSet<>(visited), bindings))
		);
	}

	private static void dfs(Map<Key<?>, Binding<?>> bindings, Set<Key<?>> visited, LinkedHashSet<Key<?>> visiting, Set<Key<?>[]> cycles, Dependency node) {
		Key<?> key = node.getKey();
		if (visited.contains(key)) {
			return;
		}
		Binding<?> binding = bindings.get(key);
		if (binding != null) {
			if (!visiting.add(key)) {
				// so at this point visiting set looks something like a -> b -> c -> d -> e -> g -> c,
				// and in the code below we just get d -> e -> g -> c out of it
				Iterator<Key<?>> backtracked = visiting.iterator();
				int skipped = 0;
				while (backtracked.hasNext() && !backtracked.next().equals(key)) { // reference equality doesn't always work here
					skipped++;
				}
				Key<?>[] cycle = new Key[visiting.size() - skipped];
				for (int i = 0; i < cycle.length - 1; i++) {
					cycle[i] = backtracked.next(); // this should be ok
				}
				cycle[cycle.length - 1] = key;
				cycles.add(cycle);
				return;
			}
			for (Dependency dependency : binding.getDependencies()) {
				dfs(bindings, visited, visiting, cycles, dependency);
			}
			visiting.remove(key);
			visited.add(key);
		} else if (node.isRequired()) {
			throw new RuntimeException("no binding for required key " + key);
		}
	}

	public static void main(String[] args) {
		Module module = new AbstractModule() {

			@Provides
			Float provide(Boolean b) {
				return 33f;
			}

			@Provides
			Boolean provide3(String s) {
				return false;
			}

			@Provides
			String provide2(Integer z) {
				return "" + z;
			}

			@Provides
			Integer provide1(Boolean b) {
				return 31;
			}
			//---


			@Provides
			@Named("second")
			Float provide123(@Named("second") Boolean b) {
				return 33f;
			}

			@Provides
			@Named("second")
			Boolean provide33(@Named("second") String s) {
				return false;
			}

			@Provides
			@Named("second")
			Integer provide12(@Named("second") Boolean b) {
				return 31;
			}

			@Provides
			@Named("second")
			String provide52(@Named("second") Integer z) {
				return "" + z;
			}
		};

		Injector injector = Injector.create(module);
//		checkBindingGraph(injector.getBindings()).forEach(x -> System.out.println(Arrays.toString(x)));
	}
}
