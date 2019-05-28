package io.datakernel.di.util;

import io.datakernel.di.*;
import io.datakernel.di.Optional;
import io.datakernel.di.util.Constructors.Factory;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.lang.annotation.Annotation;
import java.lang.reflect.*;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static io.datakernel.di.util.Utils.checkArgument;
import static java.util.stream.Collectors.toList;
import static java.util.stream.Collectors.toSet;

public final class ReflectionUtils {
	private ReflectionUtils() {
		throw new AssertionError("nope.");
	}

	public static String getShortName(Class<?> cls) {
		return cls.getName().replaceAll("(?:[^.]+\\.)*([^.]+)", "$1");
	}

	public static <T> Key<T> keyOf(@NotNull Type type, Annotation[] annotations) {
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

	public static Scope[] scopesFrom(Annotation[] annotations) {
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

	public static <T> BindingInitializer<T> injectingInitializer(Key<? extends T> returnType) {
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
				.map(parameter -> {
					Key<Object> key = keyOf(parameter.getParameterizedType(), parameter.getDeclaredAnnotations());
					return new Dependency(key, !parameter.isAnnotationPresent(Optional.class));
				})
				.toArray(Dependency[]::new);
	}

	@SuppressWarnings("unchecked")
	public static <T> Binding<T> bindingForMethod(@Nullable Object module, Method method) {
		method.setAccessible(true);

		Key<T> returnType = Key.ofType(method.getGenericReturnType());
		Dependency[] dependencies = toDependencies(method.getParameters());

		return Binding.of(dependencies, args -> {
			try {
				return (T) method.invoke(module, args);
			} catch (IllegalAccessException | InvocationTargetException e) {
				throw new RuntimeException("failed to call method " + method, e);
			}
		}, LocationInfo.from(method));
	}

	public static <T> Binding<T> bindingForConstructor(Key<T> key, Constructor<T> constructor) {
		constructor.setAccessible(true);

		Dependency[] dependencies = toDependencies(constructor.getParameters());

		return Binding.of(dependencies, args -> {
			try {
				return constructor.newInstance(args);
			} catch (InstantiationException | IllegalAccessException | InvocationTargetException e) {
				throw new RuntimeException("failed to create " + key, e);
			}
		}, LocationInfo.from(constructor));
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
								injector.getInstanceOrNull(dependency.getKey()))
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

	private static void addImplicitBindings(Set<Key<?>> visited, Trie<Scope, Map<Key<?>, Binding<?>>> bindings) {
		addImplicitBindings(visited, bindings.get());
		bindings.getChildren().values().forEach(sub -> addImplicitBindings(visited, sub));
	}

	@SuppressWarnings("unchecked")
	private static void addImplicitBindings(Set<Key<?>> checked, Map<Key<?>, Binding<?>> localBindings) {
		checked.addAll(localBindings.keySet());
		List<Binding<?>> bindingsToCheck = new ArrayList<>(localBindings.values());
		do {
			bindingsToCheck = bindingsToCheck.stream()
					.flatMap(binding -> Arrays.stream(binding.getDependencies()))
					.filter(dependency -> !checked.contains(dependency.getKey()))
					.map(dependency -> {
						Key<Object> key = (Key<Object>) dependency.getKey();
						Binding<Object> binding = generateImplicitBinding(key);
						if (binding != null) {
							localBindings.put(key, binding.apply(injectingInitializer(key)));
						}
						checked.add(key); // even if there was no binding, this will be checked in a separate dependency check
						return binding;
					})
					.filter(Objects::nonNull)
					.collect(toList());
		} while (!bindingsToCheck.isEmpty());
	}

	public static Type canonicalize(Type type) {
		if (type == null) {
			return null;
		}
		if (type instanceof ParameterizedTypeImpl) {
			return type;
		}
		if (type instanceof ParameterizedType) {
			ParameterizedType parameterized = (ParameterizedType) type;
			Type owner = canonicalize(parameterized.getOwnerType());
			Type raw = canonicalize(parameterized.getRawType());
			Type[] parameters = Arrays.stream(parameterized.getActualTypeArguments()).map(ReflectionUtils::canonicalize).toArray(Type[]::new);
			return new ParameterizedTypeImpl(owner, raw, parameters);
		}
		return type;
	}

	public static Type parameterized(Class<?> rawType, Type... parameters) {
		return new ParameterizedTypeImpl(null, rawType, Arrays.stream(parameters).map(ReflectionUtils::canonicalize).toArray(Type[]::new));
	}

	private static class ParameterizedTypeImpl implements ParameterizedType {
		@Nullable
		private final Type ownerType;
		private final Type rawType;
		private final Type[] parameters;

		public ParameterizedTypeImpl(@Nullable Type ownerType, Type rawType, Type[] parameters) {
			this.ownerType = ownerType;
			this.rawType = rawType;
			this.parameters = parameters;
		}

		@Override
		public Type[] getActualTypeArguments() {
			return parameters;
		}

		@Override
		public Type getRawType() {
			return rawType;
		}

		@Nullable
		@Override
		public Type getOwnerType() {
			return ownerType;
		}

		@Override
		public boolean equals(Object o) {
			if (this == o) {
				return true;
			}
			if (o == null || getClass() != o.getClass()) {
				return false;
			}

			ParameterizedTypeImpl that = (ParameterizedTypeImpl) o;

			return Objects.equals(ownerType, that.ownerType) && rawType.equals(that.rawType) && Arrays.equals(parameters, that.parameters);

		}

		@Override
		public int hashCode() {
			return (ownerType != null ? 961 * ownerType.hashCode() : 0) + 31 * rawType.hashCode() + Arrays.hashCode(parameters);
		}

		private String toString(Type type) {
			return type instanceof Class ? ((Class) type).getName() : type.toString();
		}

		@Override
		public String toString() {
			if (parameters.length == 0) {
				return toString(rawType);
			}
			StringBuilder sb = new StringBuilder(toString(rawType))
					.append('<')
					.append(toString(parameters[0]));
			for (int i = 1; i < parameters.length; i++) {
				sb.append(", ").append(toString(parameters[i]));
			}
			return sb.append('>').toString();
		}
	}
}
