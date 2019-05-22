package io.datakernel.di.util;

import io.datakernel.di.*;
import io.datakernel.di.Binding.Factory;
import io.datakernel.di.module.AbstractModule;
import io.datakernel.di.module.Module;
import io.datakernel.di.module.Provides;
import io.datakernel.util.RecursiveType;
import io.datakernel.util.TypeT;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.lang.annotation.Annotation;
import java.lang.reflect.*;
import java.util.*;
import java.util.function.Function;

import static io.datakernel.util.CollectorsEx.toMultimap;
import static java.util.stream.Collectors.toList;
import static java.util.stream.Collectors.toSet;

public final class ReflectionUtils {
	private ReflectionUtils() {
		throw new AssertionError("nope.");
	}

	private static Key<Object> keyOf(@NotNull Type type, Annotation[] annotations) {
		Set<Annotation> names = Arrays.stream(annotations)
				.filter(annotation -> annotation.annotationType().getAnnotation(NameAnnotation.class) != null)
				.collect(toSet());
		if (names.size() > 1) {
			throw new RuntimeException("more than one name annotation");
		}
		return names.isEmpty() ?
				Key.of(TypeT.ofType(type)) :
				Key.of(TypeT.ofType(type), names.iterator().next());
	}

	@Nullable
	private static Scope scopeOf(Annotation[] annotations) {
		Set<Annotation> scopes = Arrays.stream(annotations)
				.filter(annotation -> annotation.annotationType().getAnnotation(ScopeAnnotation.class) != null)
				.collect(toSet());
		if (scopes.size() > 1) {
			throw new RuntimeException("more than one scope annotation");
		}
		return scopes.isEmpty() ? null : Scope.of(scopes.iterator().next());
	}

	public static Map<Scope, Set<Binding<?>>> getDeclatativeBindings(Object module) {
		return Arrays.stream(module.getClass().getDeclaredMethods())
				.filter(method -> method.getAnnotation(Provides.class) != null)
				.collect(toMultimap(method -> scopeOf(method.getDeclaredAnnotations()), method -> bindingForMethod(module, method)));
	}

	private static Field[] getInjectableFields(TypeT<?> type) {
		Class<?> cls = type.getRawType();
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

	private static Dependency[] makeDependencies(TypeT<?> returnType, Parameter[] parameters, Field[] injectableFields) {
		TypeVariable<?>[] typeParameters = returnType.getRawType().getTypeParameters();
		RecursiveType[] actualTypeArguments = RecursiveType.of(returnType).getTypeParams();

		Dependency[] dependencies = new Dependency[parameters.length + injectableFields.length];
		for (int i = 0; i < parameters.length; i++) {
			Parameter parameter = parameters[i];
			dependencies[i] = new Dependency(keyOf(parameter.getParameterizedType(), parameter.getDeclaredAnnotations()), true, false);
		}
		for (int i = 0; i < injectableFields.length; i++) {
			Field field = injectableFields[i];

			Type type = field.getGenericType();
			for (int j = 0; j < typeParameters.length; j++) {
				if (type == typeParameters[j]) {
					type = actualTypeArguments[j].getType();
					break;
				}
			}
			boolean required = !field.getAnnotation(Inject.class).optional();
			dependencies[parameters.length + i] = new Dependency(keyOf(type, field.getDeclaredAnnotations()), required, true);
		}
		return dependencies;
	}

	private static void inject(Object instance, Field[] injectableFields, Object[] args) {
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
	}

	public static void inject(Object instance, Injector from) {
		Field[] injectableFields = getInjectableFields(TypeT.of(instance.getClass()));
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

	private static Binding<?> bindingForMethod(@Nullable Object instance, Method method) {
		TypeT<Object> returnType = TypeT.ofType(method.getGenericReturnType());

		Field[] injectableFields = getInjectableFields(returnType);
		Dependency[] dependencies = makeDependencies(returnType, method.getParameters(), injectableFields);

		method.setAccessible(true);

		return Binding.of(keyOf(method.getGenericReturnType(), method.getDeclaredAnnotations()), dependencies, new InjectingFactory<>(args -> {
			try {
				return method.invoke(instance, args);
			} catch (IllegalAccessException | InvocationTargetException e) {
				throw new RuntimeException("failed to call method for " + returnType, e);
			}
		}, injectableFields), new LocationInfo(instance != null ? instance.getClass() : null, method.toString()));
	}

	@SuppressWarnings("unchecked")
	private static Binding<?> bindingForConstructor(Key<?> key, Constructor<?> constructor) {
		constructor.setAccessible(true);

		Field[] injectableFields = getInjectableFields(key.getTypeT());
		Dependency[] dependencies = makeDependencies(key.getTypeT(), constructor.getParameters(), injectableFields);

		return Binding.of((Key<Object>) key, dependencies, new InjectingFactory<>(args -> {
			try {
				return constructor.newInstance();
			} catch (InstantiationException | IllegalAccessException | InvocationTargetException e) {
				throw new RuntimeException("failed to create " + key.getTypeT(), e);
			}
		}, injectableFields), new LocationInfo(null, constructor.toString()));
	}

	@Nullable
	public static Binding<?> generateImplicitBinding(Dependency dependency) {
		Class<?> cls = dependency.getKey().getTypeT().getRawType();

		if (cls == Provider.class || cls == Injector.class) {
			return null; // those two are hardcoded in the injector class, do nothing here
		}

		Inject classInjectAnnotation = cls.getAnnotation(Inject.class);

		if (classInjectAnnotation != null) {
			if (classInjectAnnotation.optional()) {
				throw new RuntimeException("inject annotation on class cannot be optional");
			}
			try {
				return bindingForConstructor(dependency.getKey(), cls.getDeclaredConstructor());
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
				return bindingForConstructor(dependency.getKey(), injectConstructors.iterator().next());
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
			throw new RuntimeException("found methods with @Inject annotation that are not public static factory methods for key " + dependency.getKey());
		}
		if (factoryMethods.size() > 1) {
			throw new RuntimeException("more than one inject factory method");
		}
		if (!factoryMethods.isEmpty()) {
			return bindingForMethod(null, factoryMethods.iterator().next());
		}

		if (dependency.isRequired()) {
			throw new RuntimeException("unsatisfied dependency " + dependency.getKey() + " with no implicit bindings");
		}
		return null;
	}

	public static void addImplicitBindings(Map<Key<?>, Binding<?>> bindings) {
		Collection<Binding<?>> bindingsToCheck = new HashSet<>(bindings.values());
		do {
			bindingsToCheck = bindingsToCheck.stream()
					.flatMap(binding -> Arrays.stream(binding.getDependencies()))

					.filter(dependency -> !bindings.containsKey(dependency.getKey()))

					.map(ReflectionUtils::generateImplicitBinding)
					.filter(Objects::nonNull)
					.peek(binding -> bindings.put(binding.getKey(), binding))
					.collect(toList());
		} while (!bindingsToCheck.isEmpty());
	}

	// throws on unsatisfied dependencies, returns list of cycles
	// this is a subject to change ofc
	public static Set<Key<?>[]> checkBindingGraph(Map<Key<?>, Binding<?>> bindings) {
		Set<Key<?>> visited = new HashSet<>();
		LinkedHashSet<Key<?>> visiting = new LinkedHashSet<>();
		Set<Key<?>[]> cycles = new HashSet<>();
		bindings.forEach((key, binding) -> dfs(bindings, visited, visiting, cycles, new Dependency(key, true, false)));
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
				// postponed dependencies allow cycles
				if (!node.isPostponed()) {
					// so at this point visiting set looks something like a -> b -> c -> d -> e -> g -> c,
					// and in the code below we just get d -> e -> g -> c out of it
					Iterator<Key<?>> backtracked = visiting.iterator();
					int skipped = 0;
					while (backtracked.hasNext() && !backtracked.next().equals(key)) { // reference equality doesn't always work here
						skipped++;
					}
					Key<?>[] cycle = new Key[visiting.size() - skipped + 1];
					for (int i = 0; i < cycle.length - 1; i++) {
						cycle[i] = backtracked.next();
					}
					cycle[cycle.length - 1] = key;
					cycles.add(cycle);
					return;
				}
			}
			for (Dependency dependency : binding.getDependencies()) {
				dfs(bindings, visited, visiting, cycles, dependency);
			}
			visiting.remove(key);
		} else if (node.isRequired()) {
			throw new RuntimeException("no binding for required key " + key);
		}
		visited.add(key);
	}

	private static class InjectingFactory<T> implements Factory<T> {
		private final Function<Object[], T> creator;
		private final Field[] injectableFields;

		public InjectingFactory(Function<Object[], T> creator, Field[] injectableFields) {
			this.creator = creator;
			this.injectableFields = injectableFields;
		}

		@Override
		public T create(Object[] args) {
			return creator.apply(args);
		}

		@Override
		public void lateinit(T instance, Object[] args) {
			inject(instance, injectableFields, args);
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
