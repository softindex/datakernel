package io.datakernel.di.util;

import io.datakernel.di.core.DIException;
import org.jetbrains.annotations.Contract;
import org.jetbrains.annotations.Nullable;

import java.lang.annotation.Annotation;
import java.lang.reflect.*;
import java.util.*;

import static java.util.stream.Collectors.joining;
import static java.util.stream.Collectors.toSet;

/**
 * This class contains reflection utilities to work with Java types.
 * Its main use is for method {@link #parameterized Types.parameterized}.
 * However, just like with {@link ReflectionUtils}, other type utility
 * methods are pretty clean too so they are left public.
 */
public final class Types {
	private Types() {}

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

	public static AnnotatedType resolveTypeVariables(AnnotatedType annotatedType, Type container) {
		Type resolvedType = resolveTypeVariables(annotatedType.getType(), container);
		if (annotatedType instanceof AnnotatedParameterizedType) {
			AnnotatedType[] aata = ((AnnotatedParameterizedType) annotatedType).getAnnotatedActualTypeArguments();
			AnnotatedType[] fixed = new AnnotatedType[aata.length];
			for (int i = 0; i < aata.length; i++) {
				fixed[i] = resolveTypeVariables(aata[i], container);
			}
			return new AnnotatedParameterizedTypeImpl(resolvedType, fixed, annotatedType.getDeclaredAnnotations());
		}
		if (annotatedType instanceof AnnotatedArrayType) {
			AnnotatedType agct = ((AnnotatedArrayType) annotatedType).getAnnotatedGenericComponentType();
			return new AnnotatedArrayTypeImpl(resolvedType, resolveTypeVariables(agct, container), annotatedType.getDeclaredAnnotations());
		}
		return annotate(resolvedType, annotatedType.getDeclaredAnnotations());
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
				return resolved;
			}
		}
		if (type instanceof ParameterizedType) {
			ParameterizedType parameterized = (ParameterizedType) type;
			return parameterizedWithOwner(
					parameterized.getOwnerType(), parameterized.getRawType(),
					Arrays.stream(parameterized.getActualTypeArguments())
							.map(parameter -> resolveTypeVariables(parameter, mapping))
							.toArray(Type[]::new));
		}
		if (type instanceof GenericArrayType) {
			Type componentType = ((GenericArrayType) type).getGenericComponentType();
			return arrayOf(resolveTypeVariables(componentType, mapping));
		}
		return type;
	}

	public static Type parameterized(Type rawType, Type... parameters) {
		return parameterizedWithOwner(null, rawType, parameters);
	}

	public static Type parameterizedWithOwner(Type ownerType, Type rawType, Type... parameters) {
		return new ParameterizedTypeImpl(ownerType, rawType, parameters);
	}

	public static Type arrayOf(Type componentType) {
		return new GenericArrayTypeImpl(componentType);
	}

	public static Annotation[] concat(Annotation[] first, Annotation[] second) {
		Annotation[] result = new Annotation[first.length + second.length];
		System.arraycopy(first, 0, result, 0, first.length);
		System.arraycopy(second, 0, result, first.length, second.length);
		return result;
	}

	public static AnnotatedType annotate(AnnotatedType type, Annotation... annotations) {
		Annotation[] concatenated = concat(type.getDeclaredAnnotations(), annotations);
		if (type instanceof AnnotatedParameterizedType) {
			AnnotatedType[] aata = ((AnnotatedParameterizedType) type).getAnnotatedActualTypeArguments();
			AnnotatedType[] fixed = new AnnotatedType[aata.length];
			for (int i = 0; i < aata.length; i++) {
				fixed[i] = annotate(aata[i]);
			}
			return new AnnotatedParameterizedTypeImpl(type.getType(), fixed, concatenated);
		}
		if (type instanceof AnnotatedArrayType) {
			AnnotatedType agct = ((AnnotatedArrayType) type).getAnnotatedGenericComponentType();
			return new AnnotatedArrayTypeImpl(type.getType(), agct, concatenated);
		}
		return new AnnotatedTypeImpl(type.getType(), concatenated);
	}

	public static AnnotatedType annotate(Type type, Annotation... annotations) {
		if (type instanceof ParameterizedType) {
			Type[] actualTypeArguments = ((ParameterizedType) type).getActualTypeArguments();
			AnnotatedType[] annotatedTypes = new AnnotatedType[actualTypeArguments.length];
			for (int i = 0; i < actualTypeArguments.length; i++) {
				annotatedTypes[i] = annotate(actualTypeArguments[i]);
			}
			return new AnnotatedParameterizedTypeImpl(type, annotatedTypes, annotations);
		}
		if (type instanceof GenericArrayType) {
			return new AnnotatedArrayTypeImpl(type, annotate(((GenericArrayType) type).getGenericComponentType()), annotations);
		}
		return new AnnotatedTypeImpl(type, annotations);
	}

	private static class ParameterizedTypeImpl implements ParameterizedType {
		@Nullable
		private final Type ownerType;
		private final Type rawType;
		private final Type[] actualTypeArguments;

		public ParameterizedTypeImpl(@Nullable Type ownerType, Type rawType, Type[] actualTypeArguments) {
			this.ownerType = ownerType;
			this.rawType = rawType;
			this.actualTypeArguments = actualTypeArguments;
		}

		@Override
		public Type[] getActualTypeArguments() {
			return actualTypeArguments;
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
			if (!(o instanceof ParameterizedType)) {
				return false;
			}
			ParameterizedType that = (ParameterizedType) o;

			if (this == that) {
				return true;
			}

			return Objects.equals(ownerType, that.getOwnerType()) &&
					Objects.equals(rawType, that.getRawType()) &&
					Arrays.equals(actualTypeArguments, that.getActualTypeArguments());
		}

		@Override
		public int hashCode() {
			return Arrays.hashCode(actualTypeArguments) ^ Objects.hashCode(ownerType) ^ Objects.hashCode(rawType);
		}

		private static String toString(Type type) {
			return type instanceof Class ? ((Class) type).getName() : type.toString();
		}

		@Override
		public String toString() {
			if (actualTypeArguments.length == 0) {
				return toString(rawType);
			}
			StringBuilder sb = new StringBuilder(toString(rawType)).append('<').append(toString(actualTypeArguments[0]));
			for (int i = 1; i < actualTypeArguments.length; i++) {
				sb.append(", ").append(toString(actualTypeArguments[i]));
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
		public String toString() {
			Type componentType = getGenericComponentType();
			StringBuilder sb = new StringBuilder();

			if (componentType instanceof Class) {
				sb.append(((Class) componentType).getName());
			} else {
				sb.append(componentType.toString());
			}
			sb.append("[]");
			return sb.toString();
		}

		@Override
		public boolean equals(Object o) {
			return o instanceof GenericArrayType && componentType.equals(((GenericArrayType) o).getGenericComponentType());
		}

		@Override
		public int hashCode() {
			return componentType.hashCode();
		}
	}

	public static class AnnotatedTypeImpl implements AnnotatedType {
		private final Type type;
		private final Map<Class<? extends Annotation>, Annotation> annotationMap = new HashMap<>();
		private final Annotation[] annotations;

		private AnnotatedTypeImpl(Type type, Annotation... annotations) {
			this.type = type;
			this.annotations = annotations;
			for (Annotation annotation : annotations) {
				annotationMap.put(annotation.annotationType(), annotation);
			}
		}

		@Override
		public Type getType() {
			return type;
		}

		@SuppressWarnings("unchecked")
		@Override
		public <T extends Annotation> T getAnnotation(Class<T> annotationClass) {
			return (T) annotationMap.get(annotationClass);
		}

		@Override
		public Annotation[] getAnnotations() {
			return annotations;
		}

		@Override
		public Annotation[] getDeclaredAnnotations() {
			return annotations;
		}
	}

	public static class AnnotatedParameterizedTypeImpl extends AnnotatedTypeImpl implements AnnotatedParameterizedType {
		private final AnnotatedType[] typeArguments;

		public AnnotatedParameterizedTypeImpl(Type type, AnnotatedType[] typeArguments, Annotation... annotations) {
			super(type, annotations);
			this.typeArguments = typeArguments;
		}

		// support Java 9+
		public AnnotatedType getAnnotatedOwnerType() {
			throw new UnsupportedOperationException("Annotated owner types are not (yet) supported");
		}

		@Override
		public AnnotatedType[] getAnnotatedActualTypeArguments() {
			return typeArguments;
		}
	}

	public static class AnnotatedArrayTypeImpl extends AnnotatedTypeImpl implements AnnotatedArrayType {
		private final AnnotatedType componentType;

		public AnnotatedArrayTypeImpl(Type type, AnnotatedType componentType, Annotation... annotations) {
			super(type, annotations);
			this.componentType = componentType;
		}

		// uhm, javadoc states that array type always returns null,
		// yet they override the default method that does that
		public AnnotatedType getAnnotatedOwnerType() {
			return null;
		}

		@Override
		public AnnotatedType getAnnotatedGenericComponentType() {
			return componentType;
		}
	}
}
