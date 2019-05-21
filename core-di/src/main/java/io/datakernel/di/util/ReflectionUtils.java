package io.datakernel.di.util;

import io.datakernel.di.*;
import io.datakernel.di.module.AbstractModule;
import io.datakernel.di.module.Module;
import io.datakernel.di.module.Provides;
import io.datakernel.util.TypeT;
import org.jetbrains.annotations.NotNull;

import java.lang.annotation.Annotation;
import java.lang.reflect.*;
import java.util.*;

import static java.util.stream.Collectors.toSet;

public final class ReflectionUtils {
	private ReflectionUtils() {
		throw new AssertionError("nope.");
	}

	@NotNull
	public static Key<Object> keyOf(@NotNull Type type, Annotation[] annotations) {
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

	public static Set<Binding<?>> getDeclatativeBindings(Object instance) {
		return Arrays.stream(instance.getClass().getDeclaredMethods())
				.filter(method -> method.getAnnotation(Provides.class) != null)
				.map(method -> bindingForMethod(instance, method))
				.collect(toSet());
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
		Type[] actualTypeArguments;

		if (returnType.getType() instanceof ParameterizedType) {
			actualTypeArguments = ((ParameterizedType) returnType.getType()).getActualTypeArguments();
		} else if (typeParameters.length > 0) {
			throw new RuntimeException("generic return type");
		} else {
			actualTypeArguments = new Type[0];
		}

		Dependency[] dependencies = new Dependency[parameters.length + injectableFields.length];
		for (int i = 0; i < parameters.length; i++) {
			Parameter parameter = parameters[i];
			dependencies[i] = new Dependency(keyOf(parameter.getParameterizedType(), parameter.getDeclaredAnnotations()), true);
		}
		for (int i = 0; i < injectableFields.length; i++) {
			Field field = injectableFields[i];

			Type type = field.getGenericType();
			for (int j = 0; j < typeParameters.length; j++) {
				if (type == typeParameters[j]) {
					type = actualTypeArguments[j];
					break;
				}
			}
			boolean required = !field.getAnnotation(Inject.class).optional();
			dependencies[parameters.length + i] = new Dependency(keyOf(type, field.getDeclaredAnnotations()), required);
		}
		return dependencies;
	}

	private static Object inject(Object instance, Field[] injectableFields, Object[] args, int argOffset) {
		for (int i = 0; i < injectableFields.length; i++) {
			Object arg = args[argOffset + i];
			if (arg != null) {
				try {
					injectableFields[i].set(instance, arg);
				} catch (IllegalAccessException e) {
					throw new RuntimeException("failed to inject field " + injectableFields[i], e);
				}
			}
		}
		return instance;
	}

	private static Binding<?> bindingForMethod(Object instance, Method method) {
		TypeT<Object> returnType = TypeT.ofType(method.getGenericReturnType());

		Field[] injectableFields = getInjectableFields(returnType);
		Dependency[] dependencies = makeDependencies(returnType, method.getParameters(), injectableFields);

		int parameterCount = method.getParameterCount();
		method.setAccessible(true);

		return new Binding<>(keyOf(method.getGenericReturnType(), method.getDeclaredAnnotations()), dependencies, args -> {
			try {
				return inject(method.invoke(instance, Arrays.copyOfRange(args, 0, parameterCount)), injectableFields, args, parameterCount);
			} catch (IllegalAccessException | InvocationTargetException e) {
				throw new RuntimeException("failed to call method for " + returnType, e);
			}
		});
	}

	@SuppressWarnings("unchecked")
	private static Binding<?> bindingForConstructor(Key<?> key, Constructor constructor) {
		constructor.setAccessible(true);

		Field[] injectableFields = getInjectableFields(key.getTypeT());
		Dependency[] dependencies = makeDependencies(key.getTypeT(), constructor.getParameters(), injectableFields);

		return new Binding<>((Key<Object>) key, dependencies, args -> {
			try {
				return inject(((Constructor<?>) constructor).newInstance(), injectableFields, args, 0);
			} catch (InstantiationException | IllegalAccessException | InvocationTargetException e) {
				throw new RuntimeException("failed to create " + key.getTypeT(), e);
			}
		});
	}

	private static Binding<?> generateImplicitBinding(Key<?> key) {
		Class<?> cls = key.getTypeT().getRawType();
		Inject classInjectAnnotation = cls.getAnnotation(Inject.class);

		if (classInjectAnnotation != null) {
			if (classInjectAnnotation.optional()) {
				throw new RuntimeException("inject annotation on class cannot be optional");
			}
			try {
				return bindingForConstructor(key, cls.getDeclaredConstructor());
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
				return bindingForConstructor(key, injectConstructors.iterator().next());
			}
		}

		Set<Method> factoryMethods = Arrays.stream(cls.getDeclaredMethods())
				.filter(method ->
						Modifier.isStatic(method.getModifiers()) &&
								Modifier.isPublic(method.getModifiers()) &&
								method.getReturnType() == cls &&
								method.getAnnotation(Inject.class) != null)
				.collect(toSet());

		if (factoryMethods.size() > 1) {
			throw new RuntimeException("more than one inject factory method");
		}
		if (!factoryMethods.isEmpty()) {
			return bindingForMethod(null, factoryMethods.iterator().next());
		}

		throw new RuntimeException("unsatisfied dependency with no implicit bindings");
	}

	public static Set<Binding<?>> generateImplicitBindings(Map<Key<?>, Binding<?>> bindings) {
		return bindings.values().stream()
				.flatMap(binding -> Arrays.stream(binding.getDependencies()))
				.filter(dependency -> dependency.isRequired() && !bindings.containsKey(dependency.getKey()))
				.map(unsatisfied -> generateImplicitBinding(unsatisfied.getKey()))
				.collect(toSet());
	}

	private static void dfs(Map<Key<?>, Binding<?>> bindings, Set<Key<?>> visited, LinkedHashSet<Key<?>> visiting, Set<List<Key<?>>> cycles, Binding<?> node) {
		Key<?> key = node.getKey();
		if (visited.contains(key)) {
			return;
		}
		if (!visiting.add(key)) {
			// so at this point visiting looks something like a -> b -> c -> d -> e -> g -> c,
			// and in the code below we just get d -> e -> g -> c out of it
			Iterator<Key<?>> backtracked = visiting.iterator();
			int skipped = 0;
			while (backtracked.hasNext() && backtracked.next() != key) {
				skipped++;
			}
			List<Key<?>> cycle = new ArrayList<>(visiting.size() - skipped + 1);
			backtracked.forEachRemaining(cycle::add);
			cycle.add(key);
			cycles.add(cycle);
			return;
		}
		for (Dependency dependency : node.getDependencies()) {
			if (dependency.isRequired()) {
				dfs(bindings, visited, visiting, cycles, bindings.get(dependency.getKey()));
			}
		}
		visiting.remove(key);
		visited.add(key);
	}

	public static Set<List<Key<?>>> findCyclicDependencies(Map<Key<?>, Binding<?>> bindings) {
		Set<Key<?>> visited = new HashSet<>();
		LinkedHashSet<Key<?>> visiting = new LinkedHashSet<>();
		Set<List<Key<?>>> cycles = new HashSet<>();
		bindings.values().forEach(binding -> dfs(bindings, visited, visiting, cycles, binding));
		return cycles;
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
			Integer provide1(Boolean b) {
				return 31;
			}

			@Provides
			String provide2(Integer z) {
				return "" + z;
			}
			//---


			@Provides @Named("second")
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
		findCyclicDependencies(injector.getBindings()).forEach(System.out::println);
	}
}
