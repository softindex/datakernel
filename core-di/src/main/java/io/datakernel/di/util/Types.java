package io.datakernel.di.util;

import io.datakernel.di.core.DIException;
import org.jetbrains.annotations.Contract;
import org.jetbrains.annotations.Nullable;

import java.lang.reflect.*;
import java.util.*;

import static java.util.stream.Collectors.joining;
import static java.util.stream.Collectors.toSet;

public final class Types {
	private Types() {
		throw new AssertionError("nope.");
	}

	public static Class<?> getRawType(Type type) {
		if (type instanceof Class) {
			return (Class<?>) type;
		}
		if (type instanceof ParameterizedType) {
			return getRawType(((ParameterizedType) type).getRawType());
		}
		if (type instanceof GenericArrayType) {
			return getRawType(((GenericArrayType) type).getGenericComponentType());
		}
		throw new IllegalArgumentException("Cannot get raw type from " + type);
	}

	public static boolean isInheritedFrom(Type type, Type from) {
		return isInheritedFrom(type, from, getGenericTypeMapping(type));
	}

	private static boolean isInheritedFrom(Type type, Type from, Map<TypeVariable<?>, Type> genericMapping) {
		if (from == Object.class) {
			return true;
		}
		if (matches(resolveTypeVariables(type, genericMapping), from)) {
			return true;
		}
		Class<?> rawType = getRawType(type);

		Type superclass = rawType.getGenericSuperclass();
		if (superclass != null && isInheritedFrom(superclass, from, genericMapping)) {
			return true;
		}
		return Arrays.stream(rawType.getGenericInterfaces())
				.anyMatch(iface -> isInheritedFrom(iface, from, genericMapping));
	}

	public static boolean matches(Type strict, Type pattern) {
		if (strict.equals(pattern)) {
			return true;
		}
		if (pattern instanceof WildcardType) {
			WildcardType wildcard = (WildcardType) pattern;
			return Arrays.stream(wildcard.getUpperBounds()).allMatch(bound -> isInheritedFrom(strict, bound))
					&& Arrays.stream(wildcard.getLowerBounds()).allMatch(bound -> isInheritedFrom(bound, strict));
		}
		if (pattern instanceof TypeVariable) {
			TypeVariable<?> typevar = (TypeVariable<?>) pattern;
			return Arrays.stream(typevar.getBounds()).allMatch(bound -> isInheritedFrom(strict, bound));
		}
		if (strict instanceof GenericArrayType && pattern instanceof GenericArrayType) {
			return matches(((GenericArrayType) strict).getGenericComponentType(), ((GenericArrayType) pattern).getGenericComponentType());
		}
		if (!(strict instanceof ParameterizedType) || !(pattern instanceof ParameterizedType)) {
			return false;
		}
		ParameterizedType parameterizedStrict = (ParameterizedType) strict;
		ParameterizedType parameterizedPattern = (ParameterizedType) pattern;

		if (parameterizedPattern.getOwnerType() != null) {
			if (parameterizedStrict.getOwnerType() == null) {
				return false;
			}
			if (!matches(parameterizedPattern.getOwnerType(), parameterizedStrict.getOwnerType())) {
				return false;
			}
		}
		if (!matches(parameterizedPattern.getRawType(), parameterizedStrict.getRawType())) {
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

	public static boolean contains(Type type, Type sub) {
		if (type.equals(sub)) {
			return true;
		}
		if (type instanceof GenericArrayType) {
			return contains(((GenericArrayType) type).getGenericComponentType(), sub);
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
		return Arrays.stream(parameterized.getActualTypeArguments())
				.anyMatch(argument -> contains(argument, sub));
	}

	@Nullable
	public static Class<?> findClosestAncestor(Class<?> real, Collection<Class<?>> patterns) {
		return patterns.stream()
				.filter(pattern -> real == pattern)
				.findFirst()
				.orElseGet(() -> {
					Class<?> superclass = real.getSuperclass();
					return superclass != null ?
							findClosestAncestor(superclass, patterns) :
							null;
				});
	}

	// pattern = Map<K, List<V>>
	// real    = Map<String, List<Integer>>
	//
	// result  = {K -> String, V -> Integer}
	public static Map<TypeVariable<?>, Type> extractMatchingGenerics(Type pattern, Type real) {
		Map<TypeVariable<?>, Type> result = new HashMap<>();
		extractMatchingGenerics(pattern, real, result);
		return result;
	}

	private static void extractMatchingGenerics(Type pattern, Type real, Map<TypeVariable<?>, Type> result) {
		if (pattern instanceof TypeVariable) {
			result.put((TypeVariable<?>) pattern, real);
			return;
		}
		if (pattern.equals(real)) {
			return;
		}
		if (pattern instanceof GenericArrayType && real instanceof GenericArrayType) {
			extractMatchingGenerics(((GenericArrayType) pattern).getGenericComponentType(), ((GenericArrayType) real).getGenericComponentType(), result);
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
		extractMatchingGenerics(parameterizedPattern.getRawType(), parameterizedReal.getRawType(), result);
		if (!Objects.equals(parameterizedPattern.getOwnerType(), parameterizedReal.getOwnerType())) {
			return;
		}
		if (parameterizedPattern.getOwnerType() != null) {
			extractMatchingGenerics(parameterizedPattern.getOwnerType(), parameterizedReal.getOwnerType(), result);
		}
		Type[] patternTypeArgs = parameterizedPattern.getActualTypeArguments();
		Type[] realTypeArgs = parameterizedReal.getActualTypeArguments();
		if (patternTypeArgs.length != realTypeArgs.length) {
			return;
		}
		for (int i = 0; i < patternTypeArgs.length; i++) {
			extractMatchingGenerics(patternTypeArgs[i], realTypeArgs[i], result);
		}
	}

	private static final Map<Type, Map<TypeVariable<?>, Type>> genericMappingCache = new HashMap<>();

	public static Map<TypeVariable<?>, Type> getGenericTypeMapping(Type container) {
		return genericMappingCache.computeIfAbsent(container, t -> {
			Map<TypeVariable<?>, @Nullable Type> mapping = new HashMap<>();
			Class<?> cls = getRawType(t);

			// first handle if given type is parameterized too
			if (t instanceof ParameterizedType) {
				Type[] typeArgs = ((ParameterizedType) t).getActualTypeArguments();
				if (typeArgs.length != 0) {
					TypeVariable<? extends Class<?>>[] typeVars = cls.getTypeParameters();
					for (TypeVariable<? extends Class<?>> typeVar : typeVars) {
						mapping.put(typeVar, null); // not putIfAbsent because those all are first puts
					}
					for (int i = 0; i < typeArgs.length; i++) {
						Type typeArg = typeArgs[i];
						mapping.put(typeVars[i], typeArg instanceof TypeVariable ? mapping.get(typeArg) : typeArg);
					}
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
			Set<TypeVariable<?>> unsatisfiedGenerics = mapping.entrySet().stream()
					.filter(e -> e.getValue() == null)
					.map(e -> (TypeVariable<?>) e.getKey())
					.collect(toSet());
			if (!unsatisfiedGenerics.isEmpty()) {
				throw new DIException(unsatisfiedGenerics.stream()
						.map(typevar -> typevar + " from " + typevar.getGenericDeclaration())
						.collect(joining(", ", "Actual types for generics [", "] were not found in class hierarchy")));
			}
			return mapping;
		});
	}

	public static Type resolveTypeVariables(Type type, Type container) {
		return resolveTypeVariables(type, getGenericTypeMapping(container));
	}

	@Contract("null, _ -> null")
	public static Type resolveTypeVariables(Type type, Map<TypeVariable<?>, Type> mapping) {
		if (type == null) {
			return null;
		}
		if (type instanceof TypeVariable) {
			Type resolved = mapping.get(type);
			if (resolved != null) {
				return ensureEquality(resolved);
			}
		}
		if (type instanceof ParameterizedType) {
			ParameterizedType parameterized = (ParameterizedType) type;
			Type owner = ensureEquality(parameterized.getOwnerType());
			Type raw = ensureEquality(parameterized.getRawType());
			Type[] parameters = Arrays.stream(parameterized.getActualTypeArguments()).map(parameter -> resolveTypeVariables(parameter, mapping)).toArray(Type[]::new);
			return new ParameterizedTypeImpl(owner, raw, parameters);
		}
		if (type instanceof GenericArrayType) {
			Type componentType = ensureEquality(((GenericArrayType) type).getGenericComponentType());
			return new GenericArrayTypeImpl(resolveTypeVariables(componentType, mapping));
		}
		return type;
	}

	@Contract("null -> null")
	public static Type ensureEquality(Type type) {
		if (type == null) {
			return null;
		}
		if (type instanceof ParameterizedTypeImpl) {
			return type;
		}
		if (type instanceof ParameterizedType) {
			ParameterizedType parameterized = (ParameterizedType) type;
			Type owner = ensureEquality(parameterized.getOwnerType());
			Type raw = ensureEquality(parameterized.getRawType());
			Type[] actualTypeArguments = parameterized.getActualTypeArguments();
			Type[] parameters = new Type[actualTypeArguments.length];
			for (int i = 0; i < actualTypeArguments.length; i++) {
				parameters[i] = ensureEquality(actualTypeArguments[i]);
			}
			return new ParameterizedTypeImpl(owner, raw, parameters);
		}
		if (type instanceof GenericArrayType) {
			return new GenericArrayTypeImpl(((GenericArrayType) type).getGenericComponentType());
		}
		return type;
	}

	public static Type parameterized(Class<?> rawType, Type... parameters) {
		return new ParameterizedTypeImpl(null, rawType, Arrays.stream(parameters).map(Types::ensureEquality).toArray(Type[]::new));
	}

	public static Type arrayOf(Type componentType) {
		return new GenericArrayTypeImpl(componentType);
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

	private static class GenericArrayTypeImpl implements GenericArrayType {
		private final Type componentType;

		public GenericArrayTypeImpl(Type componentType) {
			this.componentType = componentType;
		}

		@Override
		public Type getGenericComponentType() {
			return componentType;
		}

		@Override
		public boolean equals(Object o) {
			if (this == o) {
				return true;
			}
			if (o == null || getClass() != o.getClass()) {
				return false;
			}

			GenericArrayTypeImpl that = (GenericArrayTypeImpl) o;

			return componentType.equals(that.componentType);
		}

		@Override
		public int hashCode() {
			return componentType.hashCode();
		}

		@Override
		public String toString() {
			return (componentType instanceof Class ? ((Class) componentType).getName() : componentType.toString()) + "[]";
		}
	}
}
