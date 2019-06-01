package io.datakernel.di.util;

import io.datakernel.di.Optional;
import io.datakernel.di.*;
import io.datakernel.di.error.BadAnnotationException;
import io.datakernel.di.error.InvalidImplicitBindingException;
import org.jetbrains.annotations.Contract;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.lang.annotation.Annotation;
import java.lang.reflect.*;
import java.util.*;
import java.util.Map.Entry;
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

	public static <T> Key<T> keyOf(@Nullable Key<?> containerType, Type type, Annotation[] annotations) {
		Set<Annotation> names = Arrays.stream(annotations)
				.filter(annotation -> annotation.annotationType().isAnnotationPresent(NameAnnotation.class))
				.collect(toSet());
		if (names.size() > 1) {
			throw new BadAnnotationException(annotations, "more than one name annotation");
		}
		Type resolved = resolveGenerics(type, containerType);
		return names.isEmpty() ?
				Key.ofType(resolved) :
				Key.ofType(resolved, names.iterator().next());
	}

	public static Scope[] scopesFrom(Annotation[] annotations) {
		Set<Annotation> scopes = Arrays.stream(annotations)
				.filter(annotation -> annotation.annotationType().isAnnotationPresent(ScopeAnnotation.class))
				.collect(toSet());

		Scopes nested = (Scopes) Arrays.stream(annotations).filter(annotation -> annotation.annotationType() == Scopes.class)
				.findAny()
				.orElse(null);

		if (scopes.size() > 1) {
			throw new BadAnnotationException(annotations, "more than one scope annotation");
		}
		if (!scopes.isEmpty() && nested != null) {
			throw new BadAnnotationException(annotations, "cannot have both @Scoped and other scope annotations");
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
		// init reflection here
		return Arrays.stream(Thread.currentThread().getStackTrace())
				.skip(2)
				.filter(trace ->
						Stream.concat(Stream.of(ReflectionUtils.class), Arrays.stream(skip))
								.noneMatch(cls -> trace.getClassName().equals(cls.getName())))
				.findFirst()
				.map(LocationInfo::from)
				.orElse(null);
	}

	@SuppressWarnings("unchecked")
	public static <T> BindingInitializer<T> injectingInitializer(Key<? extends T> containingType) {
		List<BindingInitializer<T>> initializers = Stream.concat(
				getAnnotatedElements(containingType.getRawType(), Inject.class, Class::getDeclaredFields).stream()
						.map(field -> (BindingInitializer<T>) fieldInjector(containingType, field, !field.getAnnotation(Inject.class).optional())),
				getAnnotatedElements(containingType.getRawType(), Inject.class, Class::getDeclaredMethods).stream()
						.map(method -> (BindingInitializer<T>) methodInjector(containingType, method, !method.getAnnotation(Inject.class).optional())))
				.collect(toList());
		return BindingInitializer.combine(initializers);
	}

	public static <T> BindingInitializer<T> fieldInjector(Key<? extends T> container, Field field, boolean required) {
		field.setAccessible(true);

		Key<Object> key = keyOf(container, field.getGenericType(), field.getDeclaredAnnotations());
		Dependency dependency = new Dependency(key, required);

		return BindingInitializer.of(new Dependency[]{dependency}, (instance, args) -> {
			Object arg = args[0];
			if (arg == null) {
				return;
			}
			try {
				field.set(instance, arg);
			} catch (IllegalAccessException e) {
				throw new RuntimeException("failed to inject into injectable field " + field, e);
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
					throw new RuntimeException("failed to inject into injectable method " + method, e);
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
				throw new RuntimeException("failed to inject into injectable method " + method, e);
			}
		});
	}

	@NotNull
	private static Dependency[] toDependencies(@Nullable Key<?> container, Parameter[] parameters) {
		Dependency[] dependencies = new Dependency[parameters.length];

		// submitted an actual JDK bug report for this
		boolean workaround =
				parameters.length != 0
						&& parameters[0].getDeclaringExecutable() instanceof Constructor
						&& parameters[0].getDeclaringExecutable().getDeclaringClass().getEnclosingClass() != null;

		for (int i = 0; i < dependencies.length; i++) {
			Type type = parameters[i].getParameterizedType();

			Parameter parameter = parameters[workaround && i != 0 ? i - 1 : i];

			Key<Object> key = keyOf(container, type, parameter.getDeclaredAnnotations());
			dependencies[i] = new Dependency(key, !parameter.isAnnotationPresent(Optional.class));
		}
		return dependencies;
	}

	@SuppressWarnings("unchecked")
	public static <T> Binding<T> bindingForMethod(@Nullable Object module, Method method) {
		method.setAccessible(true);

		return Binding.of(toDependencies(module != null ? Key.of(module.getClass()) : null, method.getParameters()), args -> {
			try {
				return (T) method.invoke(module, args);
			} catch (IllegalAccessException | InvocationTargetException e) {
				throw new RuntimeException("failed to call method " + method, e);
			}
		}).at(LocationInfo.from(method));
	}

	@SuppressWarnings("unchecked")
	public static <T> Binding<T> bindingForGenericMethod(@Nullable Object module, Key<?> requestedKey, Method method) {
		method.setAccessible(true);

		Key<?> moduleType = module != null ? Key.of(module.getClass()) : null;

		Type genericReturnType = method.getGenericReturnType();
		Map<Type, Type> mapping = extractTypevarValues(genericReturnType, requestedKey.getType());

		Dependency[] dependencies = Arrays.stream(method.getParameters())
				.map(parameter -> {
					Key<?> paramKey = keyOf(moduleType, parameter.getParameterizedType(), parameter.getDeclaredAnnotations());
					Key<Object> fullKey = Key.ofType(resolveGenerics(paramKey.getType(), mapping), paramKey.getName());
					return new Dependency(fullKey, !parameter.isAnnotationPresent(Optional.class));
				})
				.toArray(Dependency[]::new);

		return (Binding<T>) Binding.of(dependencies,
				args -> {
					try {
						return method.invoke(module, args);
					} catch (IllegalAccessException | InvocationTargetException e) {
						throw new RuntimeException("failed to call method " + method, e);
					}
				})
				.at(LocationInfo.from(method));
	}

	public static <T> Binding<T> bindingForConstructor(Key<T> key, Constructor<T> constructor) {
		constructor.setAccessible(true);

		Dependency[] dependencies = toDependencies(null, constructor.getParameters());

		return Binding.of(dependencies, args -> {
			try {
				return constructor.newInstance(args);
			} catch (InstantiationException | IllegalAccessException | InvocationTargetException e) {
				throw new RuntimeException("failed to create " + key, e);
			}
		}).at(LocationInfo.from(constructor));
	}

	public static <T> Binding<Provider<T>> bindingForProvider(Key<T> elementKey, Binding<T> elementBinding) {
		Dependency[] dependencies = new Dependency[elementBinding.getDependencies().length + 1];

		dependencies[0] = new Dependency(Key.of(Injector.class), true);
		System.arraycopy(elementBinding.getDependencies(), 0, dependencies, 1, dependencies.length - 1);

		return Binding.of(dependencies, args -> {
			Injector injector = (Injector) args[0];
			Object[] elementArgs = Arrays.copyOfRange(args, 1, args.length);
			return new Provider<T>() {
				@Override
				public T get() {
					return elementBinding.getFactory().create(elementArgs);
				}

				@Override
				public T newInstance() {
					return injector.getInstance(elementKey);
				}
			};
		});
	}

	@SuppressWarnings("unchecked")
	@Nullable
	public static <T> Binding<T> generateImplicitBinding(Key<T> key) {
		Class<?> cls = key.getRawType();

		Inject classInjectAnnotation = cls.getAnnotation(Inject.class);

		if (classInjectAnnotation != null) {
			if (classInjectAnnotation.optional()) {
				throw new InvalidImplicitBindingException(key, "inject annotation on class cannot be optional");
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
			Set<Constructor<?>> injectConstructors = Arrays.stream(cls.getDeclaredConstructors())
					.filter(c -> {
						Inject inject = c.getAnnotation(Inject.class);
						if (inject != null) {
							if (inject.optional()) {
								throw new InvalidImplicitBindingException(key, "inject annotation on constructor cannot be optional");
							}
							return true;
						}
						return false;
					})
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

	// pattern = Map<K, List<V>>
	// real    = Map<String, List<Integer>>
	//
	// result  = {K -> String, V -> Integer}
	public static Map<Type, Type> extractTypevarValues(Type pattern, Type real) {
		Map<Type, Type> result = new HashMap<>();
		extractTypevarValues(pattern, real, result);
		return result;
	}

	private static void extractTypevarValues(Type pattern, Type real, Map<Type, Type> result) {
		if (pattern.equals(real)) {
			return;
		}
		if (pattern instanceof TypeVariable) {
			result.put(pattern, real);
			return;
		}
		if (!(pattern instanceof ParameterizedType) || !(real instanceof ParameterizedType)) {
			return;
		}
		ParameterizedType parameterizedPattern = (ParameterizedType) pattern;
		ParameterizedType parameterizedReal = (ParameterizedType) real;
		if (!parameterizedPattern.getRawType().equals(parameterizedReal.getRawType())) {
			return;
		}
		if (!Objects.equals(parameterizedPattern.getOwnerType(), parameterizedReal.getOwnerType())) {
			return;
		}
		Type[] patternTypeArgs = parameterizedPattern.getActualTypeArguments();
		Type[] realTypeArgs = parameterizedReal.getActualTypeArguments();
		if (patternTypeArgs.length != realTypeArgs.length) {
			return;
		}
		for (int i = 0; i < patternTypeArgs.length; i++) {
			extractTypevarValues(patternTypeArgs[i], realTypeArgs[i], result);
		}
	}

	private static final Map<Key<?>, Map<Type, Type>> genericMappingCache = new HashMap<>();

	public static Map<Type, Type> getTypeGenericMapping(Key<?> type) {
		return genericMappingCache.computeIfAbsent(type, t -> {
			Map<Type, @Nullable Type> mapping = new HashMap<>();
			Class<?> cls = t.getRawType();

			// first handle possible key params
			if (t.getTypeParams().length != 0) {
				TypeVariable<? extends Class<?>>[] typeVars = cls.getTypeParameters();
				for (TypeVariable<? extends Class<?>> typeVar : typeVars) {
					mapping.put(typeVar, null); // not putIfAbsent because those all are first puts
				}
				Type[] typeArgs = t.getTypeParams();
				for (int i = 0; i < typeArgs.length; i++) {
					Type typeArg = typeArgs[i];
					mapping.put(typeVars[i], typeArg instanceof TypeVariable ? mapping.get(typeArg) : typeArg);
				}
			}
			// and then tail-recursively the superclasses
			for (; ; ) {
				Type genericSuperclass = cls.getGenericSuperclass();
				cls = cls.getSuperclass();
				if (cls == Object.class || cls == null) {
					break;
				}
				TypeVariable<? extends Class<?>>[] typeVars = cls.getTypeParameters();
				for (TypeVariable<? extends Class<?>> typeVar : typeVars) {
					mapping.putIfAbsent(typeVar, null);
				}
				if (genericSuperclass instanceof ParameterizedType) {
					Type[] typeArgs = ((ParameterizedType) genericSuperclass).getActualTypeArguments();
					for (int i = 0; i < typeArgs.length; i++) {
						Type typeArg = typeArgs[i];
						mapping.put(typeVars[i], typeArg instanceof TypeVariable ? mapping.get(typeArg) : typeArg);
					}
				}
			}
			Set<Type> unsatisfiedGenerics = mapping.entrySet().stream()
					.filter(e -> e.getValue() == null)
					.map(Entry::getKey)
					.collect(toSet());
			if (!unsatisfiedGenerics.isEmpty()) {
				throw new RuntimeException("generics " + unsatisfiedGenerics + " were never specified");
			}
			return mapping;
		});
	}

	public static boolean contains(Type type, Type sub) {
		if (type.equals(sub)) {
			return true;
		}
		if (!(type instanceof ParameterizedType)) {
			return false;
		}
		ParameterizedType parameterized = (ParameterizedType) type;
		if (contains(parameterized.getRawType(), sub)) {
			return true;
		}
		if (parameterized.getOwnerType() != null && contains(parameterized.getOwnerType(), sub)) {
			return true;
		}
		Type[] typeArguments = parameterized.getActualTypeArguments();
		for (Type argument : typeArguments) {
			if (contains(argument, sub)) {
				return true;
			}
		}
		return false;
	}

	public static boolean matches(Type strict, Type pattern) {
		if (strict.equals(pattern)) {
			return true;
		}
		if (pattern instanceof WildcardType) {
			WildcardType wildcard = (WildcardType) pattern;
			if ((wildcard.getUpperBounds().length != 1 && wildcard.getUpperBounds()[0] != Object.class) || wildcard.getLowerBounds().length != 0) {
				throw new RuntimeException("Bounded wildcards are not supported yet");
			}
			return true;
		}
		if (pattern instanceof TypeVariable) {
			TypeVariable<?> typevar = (TypeVariable<?>) pattern;
			if (typevar.getBounds().length != 1 && typevar.getBounds()[0] != Object.class) {
				throw new RuntimeException("Bounded wildcards are not supported yet");
			}
			return true;
		}
		if (!(strict instanceof ParameterizedType) || !(pattern instanceof ParameterizedType)) {
			return false;
		}
		ParameterizedType parameterizedStrict = (ParameterizedType) strict;
		ParameterizedType parameterizedPattern = (ParameterizedType) pattern;

		if (!Objects.equals(parameterizedPattern.getOwnerType(), parameterizedStrict.getOwnerType())) {
			return false;
		}
		if (!parameterizedPattern.getRawType().equals(parameterizedStrict.getRawType())) {
			return false;
		}

		Type[] strictParams = parameterizedStrict.getActualTypeArguments();
		Type[] patternParams = parameterizedPattern.getActualTypeArguments();
		if (strictParams.length != patternParams.length) {
			return false;
		}
		for (int i = 0; i < strictParams.length; i++) {
			if (!matches(strictParams[i], patternParams[i])) {
				return false;
			}
		}
		return true;
	}

	public static Type resolveGenerics(Type type, @Nullable Key<?> container) {
		return container == null ? type : resolveGenerics(type, getTypeGenericMapping(container));
	}

	@Contract("null, _ -> null")
	public static Type resolveGenerics(Type type, Map<Type, Type> mapping) {
		if (type == null) {
			return null;
		}
		Type resolved = mapping.get(type);
		if (resolved != null) {
			return canonicalize(resolved);
		}
		if (type instanceof ParameterizedType) {
			ParameterizedType parameterized = (ParameterizedType) type;
			Type owner = canonicalize(parameterized.getOwnerType());
			Type raw = canonicalize(parameterized.getRawType());
			Type[] parameters = Arrays.stream(parameterized.getActualTypeArguments()).map(parameter -> resolveGenerics(parameter, mapping)).toArray(Type[]::new);
			return new ParameterizedTypeImpl(owner, raw, parameters);
		}
		return type;
	}

	@Contract("null, -> null")
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
