package io.datakernel.di.util;

import io.datakernel.di.annotation.*;
import io.datakernel.di.annotation.Optional;
import io.datakernel.di.core.*;
import io.datakernel.di.error.BadAnnotationException;
import io.datakernel.di.error.InjectionFailedException;
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
		return className.replaceAll("(?:\\p{javaJavaIdentifierPart}+\\.)*(\\p{javaJavaIdentifierPart}+)", "$1");
	}

	@Nullable
	public static Name nameOf(Annotation[] annotations) {
		Set<Annotation> names = Arrays.stream(annotations)
				.filter(annotation -> annotation.annotationType().isAnnotationPresent(NameAnnotation.class))
				.collect(toSet());
		if (names.size() > 1) {
			throw new BadAnnotationException(annotations, "More than one name annotation");
		}
		return names.isEmpty() ? null : Name.of(names.iterator().next());
	}

	public static Set<Annotation> keySetsOf(Annotation[] annotations) {
		return Arrays.stream(annotations)
				.filter(annotation -> annotation.annotationType().isAnnotationPresent(KeySetAnnotation.class))
				.collect(toSet());
	}

	public static <T> Key<T> keyOf(@Nullable Type container, Type type, Annotation[] annotations) {
		Type resolved = container != null ? Types.resolveTypeVariables(type, container) : type;
		return Key.ofType(resolved, nameOf(annotations));
	}

	public static Scope[] getScope(Annotation[] annotations) {
		Set<Annotation> scopes = Arrays.stream(annotations)
				.filter(annotation -> annotation.annotationType().isAnnotationPresent(ScopeAnnotation.class))
				.collect(toSet());

		Scopes nested = (Scopes) Arrays.stream(annotations).filter(annotation -> annotation.annotationType() == Scopes.class)
				.findAny()
				.orElse(null);

		if (scopes.size() > 1) {
			throw new BadAnnotationException(annotations, "More than one scope annotation");
		}
		if (!scopes.isEmpty() && nested != null) {
			throw new BadAnnotationException(annotations, "Cannot have both @Scoped and other scope annotations");
		}
		return nested != null ?
				Arrays.stream(nested.value()).map(Scope::of).toArray(Scope[]::new) :
				scopes.isEmpty() ?
						new Scope[0] :
						new Scope[]{Scope.of(scopes.iterator().next())};
	}

	public static <T extends AnnotatedElement> List<T> getAnnotatedElements(Class<?> cls, Class<? extends Annotation> annotationType, Function<Class<?>, T[]> extractor) {
		List<T> result = new ArrayList<>();
		while (cls != null) {
			for (T element : extractor.apply(cls)) {
				if (element.isAnnotationPresent(annotationType)) {
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
		return binding.withInitializer(generateInjectingInitializer(key));
	}

	@SuppressWarnings("unchecked")
	@Nullable
	public static <T> Binding<T> generateConstructorBinding(Key<T> key) {
		Class<?> cls = key.getRawType();

		Inject classInjectAnnotation = cls.getAnnotation(Inject.class);

		if (classInjectAnnotation != null) {
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
			Set<Constructor<?>> injectConstructors = Arrays.stream(cls.getDeclaredConstructors())
					.filter(c -> c.isAnnotationPresent(Inject.class))
					.collect(toSet());

			if (injectConstructors.size() > 1) {
				throw new InvalidImplicitBindingException(key, "more than one inject constructor");
			}
			if (!injectConstructors.isEmpty()) {
				return bindingForConstructor(key, (Constructor<T>) injectConstructors.iterator().next());
			}
		}

		Set<Method> factoryMethods = Arrays.stream(cls.getDeclaredMethods())
				.filter(method -> method.isAnnotationPresent(Inject.class)
						&& method.getReturnType() == cls
						&& Modifier.isPublic(method.getModifiers())
						&& Modifier.isStatic(method.getModifiers()))
				.collect(toSet());

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
				getAnnotatedElements(containingType.getRawType(), Inject.class, Class::getDeclaredFields).stream()
						.map(field -> (BindingInitializer<T>) fieldInjector(containingType, field, !field.isAnnotationPresent(Optional.class))),
				getAnnotatedElements(containingType.getRawType(), Inject.class, Class::getDeclaredMethods).stream()
						.map(method -> (BindingInitializer<T>) methodInjector(containingType, method, !method.isAnnotationPresent(Optional.class))))
				.collect(toList());
		return BindingInitializer.combine(initializers);
	}

	public static <T> BindingInitializer<T> fieldInjector(Key<? extends T> container, Field field, boolean required) {
		field.setAccessible(true);

		Key<Object> key = keyOf(container.getType(), field.getGenericType(), field.getDeclaredAnnotations());
		Dependency dependency = new Dependency(key, required);

		return BindingInitializer.of(new Dependency[]{dependency}, (instance, args) -> {
			Object arg = args[0];
			if (arg == null) {
				return;
			}
			try {
				field.set(instance, arg);
			} catch (IllegalAccessException e) {
				throw new InjectionFailedException(field, e);
			}
		});
	}

	public static <T> BindingInitializer<T> methodInjector(Key<? extends T> container, Method method, boolean required) {
		method.setAccessible(true);

		Dependency[] dependencies = toDependencies(container, method.getParameters());

		if (required) {
			return BindingInitializer.of(dependencies, (instance, args) -> {
				try {
					method.invoke(instance, args);
				} catch (IllegalAccessException | InvocationTargetException e) {
					throw new InjectionFailedException(method, e);
				}
			});
		}

		Dependency[] optionalDependencies = Arrays.stream(dependencies)
				.map(dependency -> new Dependency(dependency.getKey(), false))
				.toArray(Dependency[]::new);

		return BindingInitializer.of(optionalDependencies, (instance, args) -> {
			for (int i = 0; i < args.length; i++) {
				if (args[i] == null && dependencies[i].isRequired()) {
					return;
				}
			}
			try {
				method.invoke(instance, args);
			} catch (IllegalAccessException | InvocationTargetException e) {
				throw new InjectionFailedException(method, e);
			}
		});
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

			Key<Object> key = keyOf(container != null ? container.getType() : null, type, parameter.getDeclaredAnnotations());
			dependencies[i] = new Dependency(key, !parameter.isAnnotationPresent(Optional.class));
		}
		return dependencies;
	}

	@SuppressWarnings("unchecked")
	public static <T> Binding<T> bindingForMethod(@Nullable Object module, Method method) {
		method.setAccessible(true);

		return Binding.of(
				args -> {
					try {
						return (T) method.invoke(module, args);
					} catch (IllegalAccessException | InvocationTargetException e) {
						throw new ProvisionFailedException(null, method, e);
					}
				},
				toDependencies(module != null ? Key.of(module.getClass()) : null, method.getParameters())).at(LocationInfo.from(method));
	}

	@SuppressWarnings("unchecked")
	public static <T> Binding<T> bindingForGenericMethod(@Nullable Object module, Key<?> requestedKey, Method method) {
		method.setAccessible(true);

		Class<?> moduleType = module != null ? module.getClass() : null;

		Type genericReturnType = method.getGenericReturnType();
		Map<TypeVariable<?>, Type> mapping = Types.extractMatchingGenerics(genericReturnType, requestedKey.getType());

		Dependency[] dependencies = Arrays.stream(method.getParameters())
				.map(parameter -> {
					Type type = Types.resolveTypeVariables(parameter.getParameterizedType(), mapping);
					Name name = nameOf(parameter.getDeclaredAnnotations());
					return new Dependency(Key.ofType(type, name), !parameter.isAnnotationPresent(Optional.class));
				})
				.toArray(Dependency[]::new);

		return (Binding<T>) Binding.of(
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

		return Binding.of(
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
