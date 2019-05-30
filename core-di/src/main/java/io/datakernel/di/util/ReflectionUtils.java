package io.datakernel.di.util;

import io.datakernel.di.Optional;
import io.datakernel.di.*;
import io.datakernel.di.error.BadAnnotationException;
import io.datakernel.di.util.Constructors.Factory;
import org.jetbrains.annotations.Contract;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.lang.annotation.Annotation;
import java.lang.reflect.*;
import java.util.*;
import java.util.Map.Entry;
import java.util.function.Function;
import java.util.stream.Stream;

import static io.datakernel.di.util.Utils.checkArgument;
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
				.filter(annotation -> annotation.annotationType().getAnnotation(NameAnnotation.class) != null)
				.collect(toSet());
		if (names.size() > 1) {
			throw new BadAnnotationException(annotations, "more than one name annotation");
		}
		Type resolved = resolveType(type, containerType);
		return names.isEmpty() ?
				Key.ofType(resolved) :
				Key.ofType(resolved, names.iterator().next());
	}

	public static Scope[] scopesFrom(Annotation[] annotations) {
		Set<Annotation> scopes = Arrays.stream(annotations)
				.filter(annotation -> annotation.annotationType().getAnnotation(ScopeAnnotation.class) != null)
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
				if (element.getAnnotation(annotationType) != null) {
					result.add(element);
				}
			}
			cls = cls.getSuperclass();
		}
		return result;
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
		return Arrays.stream(parameters)
				.map(parameter -> {
					Key<Object> key = keyOf(container, parameter.getParameterizedType(), parameter.getDeclaredAnnotations());
					return new Dependency(key, !parameter.isAnnotationPresent(Optional.class));
				})
				.toArray(Dependency[]::new);
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

	@SuppressWarnings("unchecked")
	@Nullable
	public static <T> Binding<T> generateImplicitBinding(Key<T> key) {
		Class<?> cls = key.getRawType();

		if (cls == InstanceProvider.class) {
			Type[] typeParams = key.getTypeParams();
			checkArgument(typeParams.length == 1);

			Key<Object> subkey = Key.ofType(typeParams[0], key.getName());

			return (Binding<T>) Binding.of(new Key[]{subkey, Key.of(Injector.class)}, args -> {
				Object singleton = args[0];
				Injector injector = (Injector) args[1];

				Injector current = injector;
				Binding<?> binding = null;
				while (current != null && binding == null) {
					binding = current.getBinding(subkey);
					current = current.getParent();
				}

				assert binding != null : "singleton was provided but no binding were found in injector tree";

				Factory<?> factory = binding.getFactory();
				Object[] depInstances = Arrays.stream(binding.getDependencies())
						.map(dependency -> dependency.isRequired() ?
								injector.getInstance(dependency.getKey()) :
								injector.getInstanceOrNull(dependency.getKey()))
						.toArray(Object[]::new);

				return new InstanceProvider<Object>() {
					@Override
					public Object create() {
						return factory.create(depInstances);
					}

					@Override
					public synchronized Object provide() {
						return singleton;
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

	@SuppressWarnings("unchecked")
	public static void addImplicitBindings(Trie<Scope, Map<Key<?>, Binding<?>>> trie) {
		Set<Key<?>> checked = new HashSet<>();
		trie.bfs(bindings -> {
			checked.addAll(bindings.keySet());
			List<Binding<?>> bindingsToCheck = new ArrayList<>(bindings.values());
			do {
				bindingsToCheck = bindingsToCheck.stream()
						.flatMap(binding -> Arrays.stream(binding.getDependencies()))
						.filter(dependency -> !checked.contains(dependency.getKey()))
						.map(dependency -> {
							Key<Object> key = (Key<Object>) dependency.getKey();
							Binding<Object> binding = generateImplicitBinding(key);
							if (binding != null) {
								bindings.put(key, binding.apply(injectingInitializer(key)));
							}
							checked.add(key); // even if there was no binding, this will be checked in a separate dependency check
							return binding;
						})
						.filter(Objects::nonNull)
						.collect(toList());
			} while (!bindingsToCheck.isEmpty());
		});
	}

	private static final Map<Key<?>, Map<Type, Type>> typevarCache = new HashMap<>();

	private static Map<Type, Type> getTypeMapping(Key<?> type) {
		Map<Type, Type> typeMapping = typevarCache.computeIfAbsent(type, t -> {
			Map<Type, Type> mapping = new HashMap<>();
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
					return mapping;
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
		});
		Set<Type> unsatisfiedGenerics = typeMapping.entrySet().stream()
				.filter(e -> e.getValue() == null)
				.map(Entry::getKey)
				.collect(toSet());

		if (!unsatisfiedGenerics.isEmpty()) {
			throw new RuntimeException("generics " + unsatisfiedGenerics + " were never specified");
		}
		return typeMapping;
	}

	private static boolean matches(Type strict, Type pattern) {
		if (strict.equals(pattern)) {
			return true;
		}
		if (pattern instanceof TypeVariable) {
			if (((TypeVariable) pattern).getBounds().length != 0) {
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

	public static <T extends Float> void main(String[] args) {

		System.out.println(matches(new Key<Map<String, List<Integer>>>() {}.getType(), new Key<Map<String, List<T>>>() {}.getType()));
	}

	public static Type resolveType(Type type, @Nullable Key<?> container) {
		return container == null ? type : resolveType(type, getTypeMapping(container));
	}

	@Contract("null, _ -> null")
	public static Type resolveType(Type type, Map<Type, Type> mapping) {
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
			Type[] parameters = Arrays.stream(parameterized.getActualTypeArguments()).map(parameter -> resolveType(parameter, mapping)).toArray(Type[]::new);
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
