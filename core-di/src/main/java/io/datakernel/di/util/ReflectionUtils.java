package io.datakernel.di.util;

import io.datakernel.di.annotation.Optional;
import io.datakernel.di.annotation.*;
import io.datakernel.di.core.*;
import io.datakernel.di.impl.BindingInitializer;
import io.datakernel.di.impl.CompiledBinding;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.lang.annotation.Annotation;
import java.lang.reflect.*;
import java.util.*;
import java.util.function.Function;
import java.util.stream.Stream;

import static java.util.Collections.singleton;
import static java.util.stream.Collectors.toList;
import static java.util.stream.Collectors.toSet;

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

	public static <T> Key<T> keyOf(@Nullable Type container, Type type, AnnotatedElement annotatedElement) {
		Type resolved = container != null ? Types.resolveTypeVariables(type, container) : type;
		return Key.ofType(resolved, nameOf(annotatedElement));
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
				binding.initializeWith(generateInjectingInitializer(key)) :
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
		Key<Object> key = keyOf(container.getType(), field.getGenericType(), field);
		return BindingInitializer.of(
				singleton(Dependency.toKey(key, required)),
				compiledBindings -> {
					CompiledBinding<Object> binding = compiledBindings.get(key);
					return (instance, instances, synchronizedScope) -> {
						Object arg = binding.getInstance(instances, synchronizedScope);
						if (arg == null) {
							return;
						}
						try {
							field.set(instance, arg);
						} catch (IllegalAccessException e) {
							throw new DIException("Not allowed to set injectable field " + field, e);
						}
					};
				});
	}

	public static <T> BindingInitializer<T> methodInjector(Key<T> container, Method method) {
		method.setAccessible(true);
		Dependency[] dependencies = toDependencies(container.getType(), method.getParameters());
		return BindingInitializer.of(
				Stream.of(dependencies).collect(toSet()),
				compiledBindings -> {
					CompiledBinding[] argBindings = Stream.of(dependencies)
							.map(dependency -> compiledBindings.get(dependency.getKey()))
							.toArray(CompiledBinding[]::new);
					return (instance, instances, synchronizedScope) -> {
						Object[] args = new Object[argBindings.length];
						for (int i = 0; i < argBindings.length; i++) {
							args[i] = argBindings[i].getInstance(instances, synchronizedScope);
						}
						try {
							method.invoke(instance, args);
						} catch (IllegalAccessException e) {
							throw new DIException("Not allowed to call injectable method " + method, e);
						} catch (InvocationTargetException e) {
							throw new DIException("Failed to call injectable method " + method, e.getCause());
						}
					};
				});
	}

	@NotNull
	public static Dependency[] toDependencies(@Nullable Type container, Parameter[] parameters) {
		Dependency[] dependencies = new Dependency[parameters.length];
		if (parameters.length == 0) {
			return dependencies;
		}
		// an actual JDK bug (fixed in Java 9)
		boolean workaround = parameters[0].getDeclaringExecutable().getParameterAnnotations().length != parameters.length;
		for (int i = 0; i < dependencies.length; i++) {
			Type type = parameters[i].getParameterizedType();
			Parameter parameter = parameters[workaround && i != 0 ? i - 1 : i];
			dependencies[i] = Dependency.toKey(keyOf(container, type, parameter), !parameter.isAnnotationPresent(Optional.class));
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
				toDependencies(module != null ? module.getClass() : method.getDeclaringClass(), method.getParameters()));
		return module != null ? binding.at(LocationInfo.from(module, method)) : binding;
	}

	@SuppressWarnings("unchecked")
	public static <T> Binding<T> bindingFromGenericMethod(@Nullable Object module, Key<?> requestedKey, Method method) {
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

		Binding<T> binding = Binding.to(
				args -> {
					try {
						return (T) method.invoke(module, args);
					} catch (IllegalAccessException e) {
						throw new DIException("Not allowed to call generic method " + method + " to provide requested key " + requestedKey, e);
					} catch (InvocationTargetException e) {
						throw new DIException("Failed to call generic method " + method + " to provide requested key " + requestedKey, e.getCause());
					}
				},
				dependencies);
		return module != null ? binding.at(LocationInfo.from(module, method)) : binding;
	}

	public static <T> Binding<T> bindingFromConstructor(Key<T> key, Constructor<T> constructor) {
		constructor.setAccessible(true);

		Dependency[] dependencies = toDependencies(key.getType(), constructor.getParameters());

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
				dependencies);
	}
}
