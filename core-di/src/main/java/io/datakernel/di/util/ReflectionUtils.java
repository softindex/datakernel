package io.datakernel.di.util;

import io.datakernel.di.Scopes;
import io.datakernel.di.*;
import io.datakernel.di.module.AbstractModule;
import io.datakernel.di.module.Module;
import io.datakernel.di.module.Provides;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.lang.annotation.Annotation;
import java.lang.reflect.*;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

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
				new Scope[]{Scope.of(scopes.iterator().next())};
	}

	public static ScopedBindings getDeclatativeBindings(Object module) {
		ScopedBindings bindings = ScopedBindings.create();

		for (Method method : module.getClass().getDeclaredMethods()) {
			if (!method.isAnnotationPresent(Provides.class)) {
				continue;
			}
			Annotation[] annotations = method.getDeclaredAnnotations();
			Scope[] scopes = scopesFrom(annotations);
			bindings.resolve(scopes).add(keyOf(method.getGenericReturnType(), annotations), bindingForMethod(module, method));
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

	public static void inject(Object instance, Injector from) {
		Field[] injectableFields = getInjectableFields(instance.getClass());
		for (Field field : injectableFields) {
			Key<Object> key = keyOf(field.getGenericType(), field.getAnnotations());
			try {
				field.setAccessible(true);
				field.set(instance, field.getAnnotation(Inject.class).optional() ?
						from.getOptionalInstance(key) :
						from.getInstance(key));
			} catch (IllegalAccessException e) {
				throw new RuntimeException("failed to inject field " + field, e);
			}
		}
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

		return new BindingInitializer<>(dependencies, (instance, args) -> {
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
		}, new LocationInfo(module != null ? module.getClass() : null, method.toString()))
				.apply(injectingInitializer(returnType));
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
		}, new LocationInfo(null, constructor.toString()))
				.apply(injectingInitializer(Key.of(constructor.getDeclaringClass())));
	}

	@SuppressWarnings("unchecked")
	@Nullable
	public static <T> Binding<T> generateImplicitBinding(Key<T> key) {
		Class<?> cls = key.getRawType();

		if (cls == Provider.class || cls == Injector.class) {
			return null; // those two are hardcoded in the injector class, do nothing here
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

	public static void addImplicitBindings(Map<Key<?>, Binding<?>> bindings) {
		Collection<Binding<?>> bindingsToCheck = new HashSet<>(bindings.values());
		do {
			bindingsToCheck = bindingsToCheck.stream()
					.flatMap(binding -> Arrays.stream(binding.getDependencies()))
					.filter(dependency -> !bindings.containsKey(dependency.getKey()))
					.map(dependency -> {
						Key<?> key = dependency.getKey();
						Binding<?> binding = generateImplicitBinding(key);
						if (binding != null) {
							bindings.put(key, binding);
						} else if (dependency.isRequired()) {
							throw new RuntimeException("unsatisfied dependency " + key + " with no implicit bindings");
						}
						return binding;
					})
					.filter(Objects::nonNull)
					.collect(toList());
		} while (!bindingsToCheck.isEmpty());
	}

	// throws on unsatisfied dependencies, returns list of cycles
	// this is a subject to change ofc
	public static Set<Key<?>[]> checkBindingGraph(Map<Key<?>, Binding<?>> bindings) {
		Set<Key<?>> visited = new HashSet<>();
		LinkedHashSet<Key<?>> visiting = new LinkedHashSet<>();
		Set<Key<?>[]> cycles = new HashSet<>();
		bindings.forEach((key, binding) -> dfs(bindings, visited, visiting, cycles, new Dependency(key, true)));
		return cycles;
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
		checkBindingGraph(injector.getBindings()).forEach(x -> System.out.println(Arrays.toString(x)));
	}
}
