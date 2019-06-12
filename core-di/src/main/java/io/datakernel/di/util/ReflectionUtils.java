package io.datakernel.di.util;

import io.datakernel.di.annotation.Optional;
import io.datakernel.di.annotation.*;
import io.datakernel.di.core.*;
import io.datakernel.di.error.InjectionFailException;
import io.datakernel.di.error.InvalidAnnotationException;
import io.datakernel.di.error.InvalidImplicitBindingException;
import io.datakernel.di.error.ProvisionFailedException;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.lang.annotation.Annotation;
import java.lang.reflect.*;
import java.util.*;
import java.util.function.Function;
import java.util.stream.Stream;

import static java.util.stream.Collectors.toList;
import static java.util.stream.Collectors.toSet;

public final class ReflectionUtils {
	private ReflectionUtils() {
		throw new AssertionError("nope.");
	}

	public static String getShortName(String className) {
		return className.replaceAll("(?:\\p{javaJavaIdentifierPart}+\\.)*", "");
	}

	@Nullable
	public static Name nameOf(AnnotatedElement annotatedElement) {
		Set<Annotation> names = Arrays.stream(annotatedElement.getDeclaredAnnotations())
				.filter(annotation -> annotation.annotationType().isAnnotationPresent(NameAnnotation.class))
				.collect(toSet());
		if (names.size() > 1) {
			throw new InvalidAnnotationException(annotatedElement, "More than one name annotation");
		}
		return names.isEmpty() ? null : Name.of(names.iterator().next());
	}

	public static Set<Annotation> keySetsOf(AnnotatedElement annotatedElement) {
		return Arrays.stream(annotatedElement.getDeclaredAnnotations())
				.filter(annotation -> annotation.annotationType().isAnnotationPresent(KeySetAnnotation.class))
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

		if (scopes.size() > 1) {
			throw new InvalidAnnotationException(annotatedElement, "More than one scope annotation");
		}
		if (!scopes.isEmpty() && nested != null) {
			throw new InvalidAnnotationException(annotatedElement, "Cannot have both @Scoped and other scope annotations");
		}
		return nested != null ?
				Arrays.stream(nested.value()).map(Scope::of).toArray(Scope[]::new) :
				scopes.isEmpty() ?
						new Scope[0] :
						new Scope[]{Scope.of(scopes.iterator().next())};
	}

	public static <T extends AnnotatedElement & Member> List<T> getAnnotatedElements(Class<?> cls,
			Class<? extends Annotation> annotationType, Function<Class<?>, T[]> extractor, boolean allowStatic) {

		List<T> result = new ArrayList<>();
		while (cls != null) {
			for (T element : extractor.apply(cls)) {
				if (element.isAnnotationPresent(annotationType)) {
					if (!allowStatic && Modifier.isStatic(element.getModifiers())) {
						throw new InvalidAnnotationException(element, "@" + annotationType.getSimpleName() + " annotation is not allowed");
					}
					result.add(element);
				}
			}
			cls = cls.getSuperclass();
		}
		return result;
	}

	@Nullable
	public static LocationInfo getLocation(Class<?>... skip) {
		return Arrays.stream(Thread.currentThread().getStackTrace())
				.skip(2)
				.filter(trace ->
						Stream.concat(Stream.of(ReflectionUtils.class), Arrays.stream(skip))
								.noneMatch(cls -> trace.getClassName().equals(cls.getName())))
				.findFirst()
				.map(LocationInfo::from)
				.orElse(null);
	}

	public static <T> Binding<T> generateImplicitBinding(Key<T> key) {
		Binding<T> binding = generateConstructorBinding(key);
		if (binding == null) {
			return null;
		}
		return binding.initialize(generateInjectingInitializer(key));
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
				throw new InvalidImplicitBindingException(key, "inject annotation on class with inject constructor");
			}
			if (!factoryMethods.isEmpty()) {
				throw new InvalidImplicitBindingException(key, "inject annotation on class with inject factory method");
			}
			try {
				Class<?> enclosingClass = cls.getEnclosingClass();

				Constructor<?> constructor = enclosingClass != null && !Modifier.isStatic(cls.getModifiers()) ?
						cls.getDeclaredConstructor(enclosingClass) :
						cls.getDeclaredConstructor();

				return bindingForConstructor(key, (Constructor<T>) constructor);
			} catch (NoSuchMethodException e) {
				throw new InvalidImplicitBindingException(key, "inject annotation on class with no default constructor");
			}
		} else {
			if (injectConstructors.size() > 1) {
				throw new InvalidImplicitBindingException(key, "more than one inject constructor");
			}
			if (!injectConstructors.isEmpty()) {
				if (!factoryMethods.isEmpty()) {
					throw new InvalidImplicitBindingException(key, "both inject constructor and inject factory method are present");
				}
				return bindingForConstructor(key, (Constructor<T>) injectConstructors.iterator().next());
			}
		}

		if (factoryMethods.size() > 1) {
			throw new InvalidImplicitBindingException(key, "more than one inject factory method");
		}
		if (!factoryMethods.isEmpty()) {
			return bindingForMethod(null, factoryMethods.iterator().next());
		}
		return null;
	}

	@SuppressWarnings("unchecked")
	public static <T> BindingInitializer<T> generateInjectingInitializer(Key<? extends T> containingType) {
		List<BindingInitializer<T>> initializers = Stream.concat(
				getAnnotatedElements(containingType.getRawType(), Inject.class, Class::getDeclaredFields, false).stream()
						.map(field -> (BindingInitializer<T>) fieldInjector(containingType, field, !field.isAnnotationPresent(Optional.class))),
				getAnnotatedElements(containingType.getRawType(), Inject.class, Class::getDeclaredMethods, true).stream()
						.filter(method -> !Modifier.isStatic(method.getModifiers())) // we allow them and just filter out to allow static factory methods
						.map(method -> (BindingInitializer<T>) methodInjector(containingType, method)))
				.collect(toList());
		return BindingInitializer.combine(initializers);
	}

	public static <T> BindingInitializer<T> fieldInjector(Key<? extends T> container, Field field, boolean required) {
		field.setAccessible(true);

		Key<Object> key = keyOf(container.getType(), field.getGenericType(), field);

		return BindingInitializer.of(
				(instance, args) -> {
					Object arg = args[0];
					if (arg == null) {
						return;
					}
					try {
						field.set(instance, arg);
					} catch (IllegalAccessException e) {
						throw new InjectionFailException(field, e);
					}
				},
				new Dependency(key, required)
		);
	}

	public static <T> BindingInitializer<T> methodInjector(Key<? extends T> container, Method method) {
		method.setAccessible(true);
		return BindingInitializer.of(
				(instance, args) -> {
					try {
						method.invoke(instance, args);
					} catch (IllegalAccessException | InvocationTargetException e) {
						throw new InjectionFailException(method, e);
					}
				},
				toDependencies(container, method.getParameters())
		);
	}

	@NotNull
	private static Dependency[] toDependencies(@Nullable Key<?> container, Parameter[] parameters) {
		Dependency[] dependencies = new Dependency[parameters.length];

		if (parameters.length == 0) {
			return dependencies;
		}
		// submitted an actual JDK bug report for this
		boolean workaround = parameters[0].getDeclaringExecutable().getParameterAnnotations().length != parameters.length;

		for (int i = 0; i < dependencies.length; i++) {
			Type type = parameters[i].getParameterizedType();

			Parameter parameter = parameters[workaround && i != 0 ? i - 1 : i];

			Key<Object> key = keyOf(container != null ? container.getType() : null, type, parameter);
			dependencies[i] = new Dependency(key, !parameter.isAnnotationPresent(Optional.class));
		}
		return dependencies;
	}

	@SuppressWarnings("unchecked")
	public static <T> Binding<T> bindingForMethod(@Nullable Object module, Method method) {
		method.setAccessible(true);

		return Binding.to(
				args -> {
					try {
						return (T) method.invoke(module, args);
					} catch (IllegalAccessException | InvocationTargetException e) {
						throw new ProvisionFailedException(null, method, e);
					}
				},
				toDependencies(module != null ? Key.of(module.getClass()) : null, method.getParameters()))
				.at(LocationInfo.from(method));
	}

	@SuppressWarnings("unchecked")
	public static <T> Binding<T> bindingForGenericMethod(@Nullable Object module, Key<?> requestedKey, Method method) {
		method.setAccessible(true);

		Type genericReturnType = method.getGenericReturnType();
		Map<TypeVariable<?>, Type> mapping = Types.extractMatchingGenerics(genericReturnType, requestedKey.getType());

		Dependency[] dependencies = Arrays.stream(method.getParameters())
				.map(parameter -> {
					Type type = Types.resolveTypeVariables(parameter.getParameterizedType(), mapping);
					Name name = nameOf(parameter);
					return new Dependency(Key.ofType(type, name), !parameter.isAnnotationPresent(Optional.class));
				})
				.toArray(Dependency[]::new);

		return (Binding<T>) Binding.to(
				args -> {
					try {
						return method.invoke(module, args);
					} catch (IllegalAccessException | InvocationTargetException e) {
						throw new ProvisionFailedException(requestedKey, method, e);
					}
				},
				dependencies)
				.at(LocationInfo.from(method));
	}

	public static <T> Binding<T> bindingForConstructor(Key<T> key, Constructor<T> constructor) {
		constructor.setAccessible(true);

		Dependency[] dependencies = toDependencies(null, constructor.getParameters());

		return Binding.to(
				args -> {
					try {
						return constructor.newInstance(args);
					} catch (InstantiationException | IllegalAccessException | InvocationTargetException e) {
						throw new ProvisionFailedException(key, constructor, e);
					}
				},
				dependencies)
				.at(LocationInfo.from(constructor));
	}
}
