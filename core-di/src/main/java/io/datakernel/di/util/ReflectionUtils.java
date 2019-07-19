package io.datakernel.di.util;

import io.datakernel.di.annotation.Optional;
import io.datakernel.di.annotation.*;
import io.datakernel.di.core.*;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.lang.annotation.Annotation;
import java.lang.reflect.*;
import java.util.*;
import java.util.function.Function;
import java.util.stream.Stream;

import static java.util.Collections.singleton;
import static java.util.stream.Collectors.*;

/**
 * These are various reflection utilities that are used by the DSL.
 * While you should not use them normally, they are pretty well organized and thus are left public.
 */
public final class ReflectionUtils {
	private ReflectionUtils() {}

	public static String getShortName(String className) {
		return className.replaceAll("(?:\\p{javaJavaIdentifierPart}+\\.)*", "");
	}

	@Nullable
	public static Name nameOf(AnnotatedElement annotatedElement) {
		Set<Annotation> names = Arrays.stream(annotatedElement.getDeclaredAnnotations())
				.filter(annotation -> annotation.annotationType().isAnnotationPresent(NameAnnotation.class))
				.collect(toSet());
		switch (names.size()) {
			case 0:
				return null;
			case 1:
				return Name.of(names.iterator().next());
			default:
				throw new DIException("More than one name annotation on " + annotatedElement);
		}
	}

	public static Set<Name> keySetsOf(AnnotatedElement annotatedElement) {
		return Arrays.stream(annotatedElement.getDeclaredAnnotations())
				.filter(annotation -> annotation.annotationType().isAnnotationPresent(KeySetAnnotation.class))
				.map(Name::of)
				.collect(toSet());
	}

	public static <T> Key<T> keyOf(@Nullable Type container, AnnotatedType type) {
		Type resolved = container != null ? Types.resolveTypeVariables(type.getType(), container) : type.getType();
		return Key.ofType(resolved, nameOf(type));
	}

	public static Scope[] getScope(AnnotatedElement annotatedElement) {
		Annotation[] annotations = annotatedElement.getDeclaredAnnotations();

		Set<Annotation> scopes = Arrays.stream(annotations)
				.filter(annotation -> annotation.annotationType().isAnnotationPresent(ScopeAnnotation.class))
				.collect(toSet());

		Scopes nested = (Scopes) Arrays.stream(annotations)
				.filter(annotation -> annotation.annotationType() == Scopes.class)
				.findAny()
				.orElse(null);

		if (nested != null) {
			if (scopes.isEmpty()) {
				return Arrays.stream(nested.value()).map(Scope::of).toArray(Scope[]::new);
			}
			throw new DIException("Cannot have both @Scoped and a scope annotation on " + annotatedElement);
		}
		switch (scopes.size()) {
			case 0:
				return Scope.UNSCOPED;
			case 1:
				return new Scope[]{Scope.of(scopes.iterator().next())};
			default:
				throw new DIException("More than one scope annotation on " + annotatedElement);
		}
	}

	public static <T extends AnnotatedElement & Member> List<T> getAnnotatedElements(Class<?> cls,
			Class<? extends Annotation> annotationType, Function<Class<?>, T[]> extractor, boolean allowStatic) {

		List<T> result = new ArrayList<>();
		while (cls != null) {
			for (T element : extractor.apply(cls)) {
				if (element.isAnnotationPresent(annotationType)) {
					if (!allowStatic && Modifier.isStatic(element.getModifiers())) {
						throw new DIException("@" + annotationType.getSimpleName() + " annotation is not allowed on " + element);
					}
					result.add(element);
				}
			}
			cls = cls.getSuperclass();
		}
		return result;
	}

	public static <T> Binding<T> generateImplicitBinding(Key<T> key) {
		Binding<T> binding = generateConstructorBinding(key);
		return binding != null ?
				generateInjectingInitializer(key).apply(binding) :
				null;
	}

	@SuppressWarnings("unchecked")
	@Nullable
	public static <T> Binding<T> generateConstructorBinding(Key<T> key) {
		Class<?> cls = key.getRawType();

		Inject classInjectAnnotation = cls.getAnnotation(Inject.class);
		Set<Constructor<?>> injectConstructors = Arrays.stream(cls.getDeclaredConstructors())
				.filter(c -> c.isAnnotationPresent(Inject.class))
				.collect(toSet());
		Set<Method> factoryMethods = Arrays.stream(cls.getDeclaredMethods())
				.filter(method -> method.isAnnotationPresent(Inject.class)
						&& method.getReturnType() == cls
						&& Modifier.isStatic(method.getModifiers()))
				.collect(toSet());

		if (classInjectAnnotation != null) {
			if (!injectConstructors.isEmpty()) {
				throw failedImplicitBinding(key, "inject annotation on class with inject constructor");
			}
			if (!factoryMethods.isEmpty()) {
				throw failedImplicitBinding(key, "inject annotation on class with inject factory method");
			}
			try {
				Class<?> enclosingClass = cls.getEnclosingClass();

				Constructor<?> constructor = enclosingClass != null && !Modifier.isStatic(cls.getModifiers()) ?
						cls.getDeclaredConstructor(enclosingClass) :
						cls.getDeclaredConstructor();

				return bindingFromConstructor(key, (Constructor<T>) constructor);
			} catch (NoSuchMethodException e) {
				throw failedImplicitBinding(key, "inject annotation on class with no default constructor");
			}
		} else {
			if (injectConstructors.size() > 1) {
				throw failedImplicitBinding(key, "more than one inject constructor");
			}
			if (!injectConstructors.isEmpty()) {
				if (!factoryMethods.isEmpty()) {
					throw failedImplicitBinding(key, "both inject constructor and inject factory method are present");
				}
				return bindingFromConstructor(key, (Constructor<T>) injectConstructors.iterator().next());
			}
		}

		if (factoryMethods.size() > 1) {
			throw failedImplicitBinding(key, "more than one inject factory method");
		}
		if (!factoryMethods.isEmpty()) {
			return bindingFromMethod(null, factoryMethods.iterator().next());
		}
		return null;
	}

	private static DIException failedImplicitBinding(Key<?> requestedKey, String message) {
		return new DIException("Failed to generate implicit binding for " + requestedKey.getDisplayString() + ", " + message);
	}

	public static <T> BindingInitializer<T> generateInjectingInitializer(Key<T> container) {
		Class<T> rawType = container.getRawType();
		List<BindingInitializer<T>> initializers = Stream.concat(
				getAnnotatedElements(rawType, Inject.class, Class::getDeclaredFields, false).stream()
						.map(field -> fieldInjector(container, field, !field.isAnnotationPresent(Optional.class))),
				getAnnotatedElements(rawType, Inject.class, Class::getDeclaredMethods, true).stream()
						.filter(method -> !Modifier.isStatic(method.getModifiers())) // we allow them and just filter out to allow static factory methods
						.map(method -> methodInjector(container, method)))
				.collect(toList());
		return BindingInitializer.combine(initializers);
	}

	public static <T> BindingInitializer<T> fieldInjector(Key<T> container, Field field, boolean required) {
		field.setAccessible(true);

		Key<Object> key = keyOf(container.getType(), field.getAnnotatedType());

		return BindingInitializer.of(
				singleton(Dependency.toKey(key, required)),
				(locator, instance) -> {
					Object arg = locator.getInstanceOrNull(key);
					if (arg == null) {
						return;
					}
					try {
						field.set(instance, arg);
					} catch (IllegalAccessException e) {
						throw new DIException("Not allowed to set injectable field " + field, e);
					}
				}
		);
	}

	public static <T> BindingInitializer<T> methodInjector(Key<T> container, Method method) {
		method.setAccessible(true);
		Dependency[] deps = getDependenciesOf(container.getType(), method);
		return BindingInitializer.of(
				Arrays.stream(deps).collect(toSet()),
				(locator, instance) -> {
					try {
						method.invoke(instance, locator.getDependencies(deps));
					} catch (IllegalAccessException e) {
						throw new DIException("Not allowed to call injectable method " + method, e);
					} catch (InvocationTargetException e) {
						throw new DIException("Failed to call injectable method " + method, e.getCause());
					}
				}
		);
	}

	@NotNull
	public static Dependency[] getDependenciesOf(@Nullable Type container, Executable executable) {
		AnnotatedType[] parameterTypes = executable.getAnnotatedParameterTypes();
		Dependency[] dependencies = new Dependency[parameterTypes.length];
		if (parameterTypes.length == 0) {
			return dependencies;
		}
		Parameter[] parameters = executable.getParameters();
		// an actual JDK bug (fixed in Java 9)
		boolean workaround = executable.getParameterAnnotations().length != parameterTypes.length;
		for (int i = 0; i < dependencies.length; i++) {
			AnnotatedType type = parameterTypes[i];
			Parameter fixed = parameters[workaround && i != 0 ? i - 1 : i];
			dependencies[i] = Dependency.toKey(keyOf(container, type), !fixed.isAnnotationPresent(Optional.class));
		}
		return dependencies;
	}

	@SuppressWarnings("unchecked")
	public static <T> Binding<T> bindingFromMethod(@Nullable Object module, Method method) {
		method.setAccessible(true);

		Binding<T> binding = Binding.to(
				args -> {
					try {
						return (T) method.invoke(module, args);
					} catch (IllegalAccessException e) {
						throw new DIException("Not allowed to call method " + method, e);
					} catch (InvocationTargetException e) {
						throw new DIException("Failed to call method " + method, e.getCause());
					}
				},
				getDependenciesOf(module != null ? module.getClass() : method.getDeclaringClass(), method));
		return module != null ? binding.at(LocationInfo.from(module, method)) : binding;
	}

	@SuppressWarnings("unchecked")
	public static <T> Binding<T> bindingFromGenericMethod(Object module, Key<?> requestedKey, Method method) {
		method.setAccessible(true);

		Type genericReturnType = method.getGenericReturnType();
		Map<TypeVariable<?>, Type> mapping = Types.extractMatchingGenerics(genericReturnType, requestedKey.getType());

		Dependency[] dependencies = Arrays.stream(method.getParameters())
				.map(parameter -> {
					Type type = Types.resolveTypeVariables(parameter.getParameterizedType(), mapping);
					Name name = nameOf(parameter);
					return Dependency.toKey(Key.ofType(type, name), !parameter.isAnnotationPresent(Optional.class));
				})
				.toArray(Dependency[]::new);

		return (Binding<T>) Binding.to(
				args -> {
					try {
						return method.invoke(module, args);
					} catch (IllegalAccessException e) {
						throw new DIException("Not allowed to call generic method " + method + " to provide requested key " + requestedKey, e);
					} catch (InvocationTargetException e) {
						throw new DIException("Failed to call generic method " + method + " to provide requested key " + requestedKey, e.getCause());
					}
				},
				dependencies)
				.at(LocationInfo.from(module, method));
	}

	public static <T> Binding<T> bindingFromConstructor(Key<T> key, Constructor<T> constructor) {
		constructor.setAccessible(true);
		return Binding.to(
				args -> {
					try {
						return constructor.newInstance(args);
					} catch (InstantiationException e) {
						throw new DIException("Cannot instantiate object from the constructor " + constructor + " to provide requested key " + key, e);
					} catch (IllegalAccessException e) {
						throw new DIException("Not allowed to call constructor " + constructor + " to provide requested key " + key, e);
					} catch (InvocationTargetException e) {
						throw new DIException("Failed to call constructor " + constructor + " to provide requested key " + key, e.getCause());
					}
				},
				getDependenciesOf(key.getType(), constructor));
	}


	public static boolean isMarkerAnnotation(Class<? extends Annotation> annotationType) {
		return annotationType.getDeclaredMethods().length == 0;
	}

	/**
	 * This method creates a proxy instance for the annotation interface that you give it.
	 * <p>
	 * It seems that Java reflection (where you extract 'real' annotations) works the same way
	 * since returned instances are proxies too.
	 * <p>
	 * We use annotation equality heavily, so since reflected annotation instances are proxies by default,
	 * this either has the same efficiency or even better than making custom dummy impl for each stateful annotation,
	 * because our proxy handles proxies with no reflection and Java proxy will call reflection on equals method anyway.
	 */
	@SuppressWarnings("unchecked")
	public static <T extends Annotation> T createAnnotationInstance(Class<T> annotationType, Map<String, Object> args) {
		AnnotationInvocationHandler h = new AnnotationInvocationHandler(annotationType, args);
		return (T) Proxy.newProxyInstance(annotationType.getClassLoader(), new Class[]{annotationType}, h);
	}

	private static class AnnotationInvocationHandler implements InvocationHandler {
		private static final Object[] NO_ARGS = new Object[0];

		private final Class<? extends Annotation> type;
		private final Map<String, Object> args;
		private final Method[] methods;

		private AnnotationInvocationHandler(Class<? extends Annotation> type, Map<String, @NotNull Object> args) {
			Class<?>[] ifaces = type.getInterfaces();
			if (!type.isAnnotation() || ifaces.length != 1 || ifaces[0] != Annotation.class) {
				throw new IllegalArgumentException("Cannot create annotation instance for non-annotation type");
			}

			List<String> misses = Arrays.stream(methods = type.getDeclaredMethods())
					.filter(method -> !args.containsKey(method.getName()))
					.map(Method::getName)
					.collect(toList());
			if (!misses.isEmpty()) {
				throw new IllegalArgumentException(misses.stream()
						.collect(joining(", ", "Annotation arguments do not contain a value for [", "] parameters")));
			}

			this.type = type;
			this.args = args;
		}

		@Override
		public Object invoke(Object proxy, Method method, Object[] args) {
			String name = method.getName();

			switch (name) {
				case "equals":
					return proxiedEquals(args[0]);
				case "hashCode":
					return proxiedHashCode();
				case "toString":
					return proxiedToString();
				case "annotationType":
					return type;
			}

			Object result = this.args.get(name);
			if (result == null) {
				throw new IllegalStateException("Annotation parameter is null, this should not happen");
			}

			if (result.getClass().isArray() && Array.getLength(result) != 0) {
				result = arrayClone(result);
			}

			return result;
		}

		private Boolean proxiedEquals(Object that) {
			if (that == this) {
				return true;
			}
			if (!type.isInstance(that)) {
				return false;
			}
			if (Proxy.isProxyClass(that.getClass())) {
				InvocationHandler handler = Proxy.getInvocationHandler(that);

				// super-fast route (our own proxy)
				if (handler instanceof AnnotationInvocationHandler) {
					AnnotationInvocationHandler exactHandler = (AnnotationInvocationHandler) handler;
					for (Method method : methods) {
						String name = method.getName();
						if (!argEquals(args.get(name), exactHandler.args.get(name))) {
							return false;
						}
					}
					return true;
				}
				// ok route (annotations that we get with java reflection are proxies)
				for (Method method : methods) {
					Object value = args.get(method.getName());
					Object other;
					try {
						other = handler.invoke(that, method, NO_ARGS);
					} catch (Throwable throwable) {
						return false;
					}
					if (!argEquals(value, other)) {
						return false;
					}
				}
				return true;
			}

			// slowest but universal route (eg somebody implemented annotation interface themselves)
			for (Method method : methods) {
				String name = method.getName();
				Object value = args.get(name);
				Object other;
				try {
					other = method.invoke(that);
				} catch (IllegalAccessException e) {
					throw new AssertionError("Annotation methods should be public", e);
				} catch (InvocationTargetException e) {
					return false;
				}
				if (!argEquals(value, other)) {
					return false;
				}
			}
			return true;
		}

		private static boolean argEquals(Object value, Object other) {
			Class<?> type = value.getClass();
			if (!type.isArray()) {
				return value.equals(other);
			}
			if (value instanceof Object[] && other instanceof Object[]) {
				return Arrays.equals((Object[]) value, (Object[]) other);
			}
			if (other.getClass() != type) {
				return false;
			}
			return primitiveArrayEquals(value, other);
		}

		private int proxiedHashCode() {
			int hash = 0;
			for (Map.Entry<String, Object> e : args.entrySet()) {
				Object value = e.getValue();
				hash += (127 * e.getKey().hashCode()) ^ (value.getClass().isArray() ? arrayHashCode(value) : value.hashCode());
			}
			return hash;
		}

		private String proxiedToString() {
			return args.entrySet().stream()
					.map(e -> {
						Object value = e.getValue();
						return e.getKey() + '=' + (value.getClass().isArray() ? arrayToString(value) : value.toString());
					})
					.collect(joining(", ", '@' + type.getName() + '(', ")"));
		}

		// region primitive array methods

		private static String arrayToString(Object array) {
			Class<?> type = array.getClass();

			// @formatter:off
			if (type == byte[].class)    return Arrays.toString((byte[]) array);
			if (type == char[].class)    return Arrays.toString((char[]) array);
			if (type == double[].class)  return Arrays.toString((double[]) array);
			if (type == float[].class)   return Arrays.toString((float[]) array);
			if (type == int[].class)     return Arrays.toString((int[]) array);
			if (type == long[].class)    return Arrays.toString((long[]) array);
			if (type == short[].class)   return Arrays.toString((short[]) array);
			if (type == boolean[].class) return Arrays.toString((boolean[]) array);
			// @formatter:on

			return Arrays.toString((Object[]) array);
		}

		private static int arrayHashCode(Object array) {
			Class<?> type = array.getClass();

			// @formatter:off
			if (type == byte[].class)    return Arrays.hashCode((byte[]) array);
			if (type == char[].class)    return Arrays.hashCode((char[]) array);
			if (type == double[].class)  return Arrays.hashCode((double[]) array);
			if (type == float[].class)   return Arrays.hashCode((float[]) array);
			if (type == int[].class)     return Arrays.hashCode((int[]) array);
			if (type == long[].class)    return Arrays.hashCode((long[]) array);
			if (type == short[].class)   return Arrays.hashCode((short[]) array);
			if (type == boolean[].class) return Arrays.hashCode((boolean[]) array);
			// @formatter:on

			return Arrays.hashCode((Object[]) array);
		}

		private static Object arrayClone(Object array) {
			Class<?> type = array.getClass();

			// @formatter:off
			if (type == byte[].class)    return ((byte[])array).clone();
			if (type == char[].class)    return ((char[])array).clone();
			if (type == double[].class)  return ((double[])array).clone();
			if (type == float[].class)   return ((float[])array).clone();
			if (type == int[].class)     return ((int[])array).clone();
			if (type == long[].class)    return ((long[])array).clone();
			if (type == short[].class)   return ((short[])array).clone();
			if (type == boolean[].class) return ((boolean[])array).clone();
			// @formatter:on

			return ((Object[]) array).clone();
		}

		private static boolean primitiveArrayEquals(Object first, Object second) {
			Class<?> type = first.getClass();

			// @formatter:off
			if (type == byte[].class)    return Arrays.equals((byte[]) first, (byte[]) second);
			if (type == char[].class)    return Arrays.equals((char[]) first, (char[]) second);
			if (type == double[].class)  return Arrays.equals((double[]) first, (double[]) second);
			if (type == float[].class)   return Arrays.equals((float[]) first, (float[]) second);
			if (type == int[].class)     return Arrays.equals((int[]) first, (int[]) second);
			if (type == long[].class)    return Arrays.equals((long[]) first, (long[]) second);
			if (type == short[].class)   return Arrays.equals((short[]) first, (short[]) second);
			if (type == boolean[].class) return Arrays.equals((boolean[]) first, (boolean[]) second);
			// @formatter:on

			throw new AssertionError("unreachable");
		}

		// endregion
	}
}
