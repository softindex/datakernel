/*
 * Copyright (C) 2015-2018 SoftIndex LLC.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.datakernel.serializer;

import io.datakernel.asm.Annotations;
import io.datakernel.codegen.ClassBuilder;
import io.datakernel.codegen.DefiningClassLoader;
import io.datakernel.codegen.Expression;
import io.datakernel.serializer.TypedModsMap.Builder;
import io.datakernel.serializer.annotations.*;
import io.datakernel.serializer.asm.*;
import io.datakernel.serializer.asm.SerializerGen.VersionsCollector;
import io.datakernel.serializer.asm.SerializerGenBuilder.SerializerForType;
import io.datakernel.serializer.util.BinaryInput;
import io.datakernel.serializer.util.BinaryOutput;
import io.datakernel.serializer.util.BinaryOutputUtils;
import io.datakernel.util.Preconditions;
import org.jetbrains.annotations.Nullable;

import java.lang.annotation.Annotation;
import java.lang.reflect.*;
import java.net.Inet4Address;
import java.net.Inet6Address;
import java.net.InetAddress;
import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;

import static io.datakernel.codegen.Expressions.*;
import static io.datakernel.util.Preconditions.check;
import static io.datakernel.util.Preconditions.checkNotNull;
import static java.lang.reflect.Modifier.*;
import static java.util.Arrays.asList;

/**
 * Scans fields of classes for serialization.
 */
public final class SerializerBuilder {
	private final AtomicInteger counter = new AtomicInteger();
	private final DefiningClassLoader definingClassLoader;
	private String profile;
	private int version = Integer.MAX_VALUE;
	private Path saveBytecodePath;
	private CompatibilityLevel compatibilityLevel = CompatibilityLevel.LEVEL_3;

	private final Map<Class<?>, SerializerGenBuilder> typeMap = new LinkedHashMap<>();
	private final Map<Class<? extends Annotation>, Class<? extends Annotation>> annotationsExMap = new LinkedHashMap<>();
	private final Map<Class<? extends Annotation>, AnnotationHandler<?, ?>> annotationsMap = new LinkedHashMap<>();
	private final Map<String, Collection<Class<?>>> extraSubclassesMap = new HashMap<>();

	private final Map<Key, SerializerGen> cachedSerializers = new HashMap<>();
	private final List<Runnable> initTasks = new ArrayList<>();

	@FunctionalInterface
	public interface Helper {
		SerializerGen createSubclassesSerializer(Class<?> type, SerializeSubclasses serializeSubclasses);
	}

	private final Helper helper = this::createSubclassesSerializer;

	private SerializerBuilder(DefiningClassLoader definingClassLoader) {
		this.definingClassLoader = definingClassLoader;
	}

	public static SerializerBuilder create(ClassLoader classLoader) {
		return create(DefiningClassLoader.create(classLoader));
	}

	public static SerializerBuilder create(String profile, ClassLoader classLoader) {
		return create(DefiningClassLoader.create(classLoader)).withProfile(profile);
	}

	public static SerializerBuilder create(String profile, DefiningClassLoader definingClassLoader) {
		return create(definingClassLoader).withProfile(profile);
	}

	public static SerializerBuilder create(DefiningClassLoader definingClassLoader) {
		SerializerBuilder builder = new SerializerBuilder(definingClassLoader);

		builder.setSerializer(Object.class, (type, generics, fallback) -> {
			check(type.getTypeParameters().length == generics.length, "Number of type parameters should be equal to number of generics");
			check(fallback == null, "Fallback must be null");
			SerializerGenClass serializer;
			SerializeInterface annotation = Annotations.findAnnotation(SerializeInterface.class, type.getAnnotations());
			if (annotation != null && annotation.impl() != void.class) {
				serializer = new SerializerGenClass(type, generics, annotation.impl());
			} else {
				serializer = new SerializerGenClass(type, generics);
			}
			builder.initTasks.add(() -> builder.scanAnnotations(type, generics, serializer));
			return serializer;
		});
		builder.setSerializer(List.class, (type, generics, fallback) -> {
			check(generics.length == 1, "List must have 1 generic type parameter");
			return new SerializerGenList(generics[0].serializer);
		});
		builder.setSerializer(Collection.class, (type, generics, fallback) -> {
			check(generics.length == 1, "Collection must have 1 generic type parameter");
			return new SerializerGenList(generics[0].serializer);
		});
		builder.setSerializer(Set.class, (type, generics, fallback) -> {
			check(generics.length == 1, "Set must have 1 generic type parameter");
			return new SerializerGenSet(generics[0].serializer);
		});
		builder.setSerializer(Map.class, (type, generics, fallback) -> {
			check(generics.length == 2, "Map must have 2 generic type parameter");
			return new SerializerGenMap(generics[0].serializer, generics[1].serializer);
		});
		builder.setSerializer(Enum.class, (type, generics, fallback) -> {
			List<FoundSerializer> foundSerializers = builder.scanSerializers(type, generics);
			if (!foundSerializers.isEmpty()) {
				SerializerGenClass serializer = new SerializerGenClass(type);
				builder.initTasks.add(() -> builder.scanAnnotations(type, generics, serializer));
				return serializer;
			} else {
				return new SerializerGenEnum(type);
			}
		});
		builder.setSerializer(Boolean.TYPE, new SerializerGenBoolean());
		builder.setSerializer(Character.TYPE, new SerializerGenChar());
		builder.setSerializer(Byte.TYPE, new SerializerGenByte());
		builder.setSerializer(Short.TYPE, new SerializerGenShort());
		builder.setSerializer(Integer.TYPE, new SerializerGenInt(false));
		builder.setSerializer(Long.TYPE, new SerializerGenLong(false));
		builder.setSerializer(Float.TYPE, new SerializerGenFloat());
		builder.setSerializer(Double.TYPE, new SerializerGenDouble());
		builder.setSerializer(Boolean.class, new SerializerGenBoolean());
		builder.setSerializer(Character.class, new SerializerGenChar());
		builder.setSerializer(Byte.class, new SerializerGenByte());
		builder.setSerializer(Short.class, new SerializerGenShort());
		builder.setSerializer(Integer.class, new SerializerGenInt(false));
		builder.setSerializer(Long.class, new SerializerGenLong(false));
		builder.setSerializer(Float.class, new SerializerGenFloat());
		builder.setSerializer(Double.class, new SerializerGenDouble());
		builder.setSerializer(String.class, new SerializerGenString());
		builder.setSerializer(Inet4Address.class, SerializerGenInet4Address.instance());
		builder.setSerializer(Inet6Address.class, SerializerGenInet6Address.instance());

		LinkedHashMap<Class<?>, SerializerGen> addressMap = new LinkedHashMap<>();
		addressMap.put(Inet4Address.class, SerializerGenInet4Address.instance());
		addressMap.put(Inet6Address.class, SerializerGenInet6Address.instance());
		builder.setSerializer(InetAddress.class, new SerializerGenSubclass(InetAddress.class, addressMap, 0));

		builder.setSerializer(ByteBuffer.class, new SerializerGenByteBuffer());

		builder.setAnnotationHandler(SerializerClass.class, SerializerClassEx.class, new SerializerClassHandler());
		builder.setAnnotationHandler(SerializeFixedSize.class, SerializeFixedSizeEx.class, new SerializeFixedSizeHandler());
		builder.setAnnotationHandler(SerializeVarLength.class, SerializeVarLengthEx.class, new SerializeVarLengthHandler());
		builder.setAnnotationHandler(SerializeSubclasses.class, SerializeSubclassesEx.class, new SerializeSubclassesHandler());
		builder.setAnnotationHandler(SerializeNullable.class, SerializeNullableEx.class, new SerializeNullableHandler());
		builder.setAnnotationHandler(SerializeStringFormat.class, SerializeStringFormatEx.class, new SerializeStringFormatHandler());
		return builder;
	}

	private <A extends Annotation, P extends Annotation> SerializerBuilder setAnnotationHandler(Class<A> annotation,
			Class<P> annotationPlural,
			AnnotationHandler<A, P> annotationHandler) {
		annotationsMap.put(annotation, annotationHandler);
		if (annotationPlural != null) {
			annotationsExMap.put(annotation, annotationPlural);
		}
		return this;
	}

	public SerializerBuilder withCompatibilityLevel(CompatibilityLevel compatibilityLevel) {
		this.compatibilityLevel = compatibilityLevel;
		return this;
	}

	/**
	 * Allows to save generated bytecode in file at provided {@code path}
	 *
	 * @param path defines where generated bytecode will be stored
	 */
	public SerializerBuilder withSaveBytecodePath(Path path) {
		this.saveBytecodePath = path;
		return this;
	}

	public SerializerBuilder withVersion(int version) {
		this.version = version;
		return this;
	}

	public SerializerBuilder withDefaultStringFormat(StringFormat format) {
		setSerializer(String.class, new SerializerGenString(format));
		return this;
	}

	public SerializerBuilder withProfile(String profile) {
		this.profile = profile;
		return this;
	}

	private void setSerializer(Class<?> type, SerializerGen serializer) {
		setSerializer(type, new SerializerGenBuilderConst(serializer));
	}

	private void setSerializer(Class<?> type, SerializerGenBuilder serializer) {
		typeMap.put(type, serializer);
	}

	public SerializerBuilder withSerializer(Class<?> type, SerializerGenBuilder serializer) {
		typeMap.put(type, serializer);
		return this;
	}

	public SerializerBuilder withSerializer(Class<?> type, SerializerGen serializer) {
		return withSerializer(type, new SerializerGenBuilderConst(serializer));
	}

	public SerializerBuilder withSubclasses(String subclassesId, List<Class<?>> subclasses) {
		extraSubclassesMap.put(subclassesId, subclasses);
		return this;
	}

	public void setSubclasses(String subclassesId, List<Class<?>> subclasses) {
		extraSubclassesMap.put(subclassesId, subclasses);
	}

	public SerializerBuilder withSubclasses(String extraSubclassesId, Class<?>... subclasses) {
		return withSubclasses(extraSubclassesId, Arrays.asList(subclasses));
	}

	public <T> void setSubclasses(Class<T> type, List<Class<? extends T>> subclasses) {
		LinkedHashSet<Class<?>> subclassesSet = new LinkedHashSet<>(subclasses);
		check(subclassesSet.size() == subclasses.size(), "Subclasses should be unique");
		SerializerGen subclassesSerializer = createSubclassesSerializer(type, subclassesSet, 0);
		setSerializer(type, subclassesSerializer);
	}

	@SafeVarargs
	public final <T> void setSubclasses(Class<T> type, Class<? extends T>... subclasses) {
		setSubclasses(type, Arrays.asList(subclasses));
	}

	public <T> SerializerBuilder withSubclasses(Class<T> type, List<Class<? extends T>> subclasses) {
		setSubclasses(type, subclasses);
		return this;
	}

	@SafeVarargs
	public final <T> SerializerBuilder withSubclasses(Class<T> type, Class<? extends T>... subclasses) {
		setSubclasses(type, subclasses);
		return this;
	}

	/**
	 * Creates a {@code BinarySerializer} for the given type token.
	 *
	 * @return {@code BinarySerializer} for the given type token
	 */
	public <T> BinarySerializer<T> build(Class<T> type) {
		SerializerForType[] serializerForTypes = new SerializerForType[0];
		return build(type, serializerForTypes);
	}

	public <T> BinarySerializer<T> build(SerializerGen serializerGen) {
		return buildBufferSerializer(serializerGen, version);
	}

	public <T> BinarySerializer<T> build(Class<?> type, SerializerForType[] generics) {
		return buildBufferSerializer(createSerializerGen(type, generics, Collections.emptyList()), version);
	}

	private SerializerGen createSerializerGen(Class<?> type, SerializerForType[] generics, List<SerializerGenBuilder> mods) {
		Key key = new Key(type, generics, mods);
		SerializerGen serializer = cachedSerializers.get(key);
		if (serializer == null) {
			serializer = createNewSerializer(type, generics, mods);
			cachedSerializers.put(key, serializer);
		}
		while (!initTasks.isEmpty()) {
			initTasks.remove(0).run();
		}
		return serializer;
	}

	private SerializerGen createNewSerializer(Class<?> type, SerializerForType[] generics, List<SerializerGenBuilder> mods) {
		if (!mods.isEmpty()) {
			SerializerGen serializer = createSerializerGen(type, generics, mods.subList(0, mods.size() - 1));
			SerializerGenBuilder last = mods.get(mods.size() - 1);
			return last.serializer(type, generics, serializer);
		}

		if (type.isArray()) {
			check(generics.length == 1, "Number of generics should be equal to 1");
			SerializerGen itemSerializer = generics[0].serializer;
			return new SerializerGenArray(itemSerializer, type);
		}

		SerializeSubclasses serializeSubclasses = Annotations.findAnnotation(SerializeSubclasses.class, type.getAnnotations());
		if (serializeSubclasses != null) {
			return createSubclassesSerializer(type, serializeSubclasses);
		}

		Class<?> key = findKey(type, typeMap.keySet());
		SerializerGenBuilder builder = typeMap.get(key);
		if (builder == null) {
			throw new IllegalArgumentException("No builder for type " + key);
		}
		SerializerGen serializer = builder.serializer(type, generics, null);
		checkNotNull(serializer);
		return serializer;
	}

	private SerializerGen createSubclassesSerializer(Class<?> type, SerializeSubclasses serializeSubclasses) {
		LinkedHashSet<Class<?>> subclassesSet = new LinkedHashSet<>(Arrays.asList(serializeSubclasses.value()));
		check(subclassesSet.size() == serializeSubclasses.value().length, "Subclasses should be unique");

		if (!serializeSubclasses.extraSubclassesId().isEmpty()) {
			Collection<Class<?>> registeredSubclasses = extraSubclassesMap.get(serializeSubclasses.extraSubclassesId());
			if (registeredSubclasses != null) {
				subclassesSet.addAll(registeredSubclasses);
			}
		}
		return createSubclassesSerializer(type, subclassesSet, serializeSubclasses.startIndex());
	}

	private SerializerGen createSubclassesSerializer(Class<?> type, LinkedHashSet<Class<?>> subclassesSet,
			int startIndex) {
		checkNotNull(subclassesSet);
		check(!subclassesSet.isEmpty(), "Set of subclasses should not be empty");
		LinkedHashMap<Class<?>, SerializerGen> subclasses = new LinkedHashMap<>();
		for (Class<?> subclass : subclassesSet) {
			check(subclass.getTypeParameters().length == 0, "Subclass should have no type parameters");
			check(type.isAssignableFrom(subclass), "Unrelated subclass '%s' for '%s'", subclass, type);

			subclasses.put(subclass, createSerializerGen(
					subclass,
					new SerializerForType[]{},
					Collections.emptyList()));
		}
		return new SerializerGenSubclass(type, subclasses, startIndex);
	}

	@Nullable
	private static Class<?> findKey(Class<?> classType, Set<Class<?>> classes) {
		Class<?> foundKey = null;
		for (Class<?> key : classes) {
			if (key.isAssignableFrom(classType)) {
				if (foundKey == null || foundKey.isAssignableFrom(key)) {
					foundKey = key;
				}
			}
		}
		return foundKey;
	}

	@SuppressWarnings({"unchecked", "rawtypes"})
	private TypedModsMap extractMods(Annotation[] annotations) {
		Builder rootBuilder = TypedModsMap.builder();
		if (annotations.length == 0) {
			return rootBuilder.build();
		}
		for (Class<? extends Annotation> annotationType : annotationsMap.keySet()) {
			Class<? extends Annotation> annotationExType = annotationsExMap.get(annotationType);
			AnnotationHandler annotationHandler = annotationsMap.get(annotationType);
			for (Annotation annotation : annotations) {
				if (annotation.annotationType() == annotationType) {
					SerializerGenBuilder serializerGenBuilder = annotationHandler.createBuilder(helper, annotation, compatibilityLevel);
					Builder child = rootBuilder.ensureChild(annotationHandler.extractPath(annotation));
					child.add(serializerGenBuilder);
				}
			}
			for (Annotation annotationEx : annotations) {
				if (annotationEx.annotationType() == annotationExType) {
					for (Annotation annotation : annotationHandler.extractList(annotationEx)) {
						SerializerGenBuilder serializerGenBuilder = annotationHandler.createBuilder(helper, annotation, compatibilityLevel);
						Builder child = rootBuilder.ensureChild(annotationHandler.extractPath(annotation));
						child.add(serializerGenBuilder);
					}
				}
			}
		}
		return rootBuilder.build();
	}

	@SuppressWarnings("rawtypes")
	public SerializerForType resolveSerializer(Class<?> classType, SerializerForType[] classGenerics, Type genericType, TypedModsMap typedModsMap) {
		if (genericType instanceof TypeVariable) {
			String typeVariableName = ((TypeVariable) genericType).getName();

			int i;
			for (i = 0; i < classType.getTypeParameters().length; i++) {
				TypeVariable<?> typeVariable = classType.getTypeParameters()[i];
				if (typeVariableName.equals(typeVariable.getName())) {
					break;
				}
			}
			check(i < classType.getTypeParameters().length, "No type variable '%s' is found in type parameters of %s", typeVariableName, classType);

			SerializerGen serializer = typedModsMap.rewrite(classGenerics[i].rawType, new SerializerForType[]{}, classGenerics[i].serializer);
			return new SerializerForType(classGenerics[i].rawType, serializer);
		} else if (genericType instanceof ParameterizedType) {
			ParameterizedType parameterizedType = (ParameterizedType) genericType;

			SerializerForType[] typeArguments = new SerializerForType[parameterizedType.getActualTypeArguments().length];
			for (int i = 0; i < parameterizedType.getActualTypeArguments().length; i++) {
				Type typeArgument = parameterizedType.getActualTypeArguments()[i];
				if (typeArgument instanceof WildcardType) {
					throw new IllegalArgumentException("Wildcard types are not supported");
				}
				typeArguments[i] = resolveSerializer(classType, classGenerics, typeArgument, typedModsMap.get(i));
			}

			Class<?> rawType = (Class<?>) parameterizedType.getRawType();
			SerializerGen serializer = createSerializerGen(rawType, typeArguments, typedModsMap.getMods());
			return new SerializerForType(rawType, serializer);
		} else if (genericType instanceof GenericArrayType) {
			throw new UnsupportedOperationException();
		} else if (genericType instanceof Class<?>) {
			Class<?> rawType = (Class<?>) genericType;
			SerializerForType[] generics = {};
			if (rawType.isArray()) {
				Class<?> componentType = rawType.getComponentType();
				SerializerForType forType = resolveSerializer(classType, classGenerics, componentType, typedModsMap.get(0));
				generics = new SerializerForType[]{forType};
			}
			SerializerGen serializer = createSerializerGen(rawType, generics, typedModsMap.getMods());
			return new SerializerForType(rawType, serializer);
		} else {
			throw new IllegalArgumentException("Unsupported type " + genericType);
		}

	}

	private static final class FoundSerializer implements Comparable<FoundSerializer> {
		final Object methodOrField;
		final int order;
		final int added;
		final int removed;
		final TypedModsMap mods;
		SerializerGen serializerGen;

		private FoundSerializer(Object methodOrField, int order, int added, int removed, TypedModsMap mods) {
			this.methodOrField = methodOrField;
			this.order = order;
			this.added = added;
			this.removed = removed;
			this.mods = mods;
		}

		public String getName() {
			if (methodOrField instanceof Field) {
				return ((Field) methodOrField).getName();
			}
			if (methodOrField instanceof Method) {
				return ((Method) methodOrField).getName();
			}
			throw new AssertionError();
		}

		private int fieldRank() {
			if (methodOrField instanceof Field) {
				return 1;
			}
			if (methodOrField instanceof Method) {
				return 2;
			}
			throw new AssertionError();
		}

		@Override
		public int compareTo(FoundSerializer o) {
			int result = Integer.compare(this.order, o.order);
			if (result != 0) {
				return result;
			}
			result = Integer.compare(fieldRank(), o.fieldRank());
			if (result != 0) {
				return result;
			}
			result = getName().compareTo(o.getName());
			return result;
		}

		@Override
		public String toString() {
			return methodOrField.getClass().getSimpleName() + " " + getName();
		}
	}

	@Nullable
	private FoundSerializer findAnnotations(Object methodOrField, Annotation[] annotations) {
		TypedModsMap mods = extractMods(annotations);

		int added = Serialize.DEFAULT_VERSION;
		int removed = Serialize.DEFAULT_VERSION;

		Serialize serialize = Annotations.findAnnotation(Serialize.class, annotations);
		if (serialize != null) {
			added = serialize.added();
			removed = serialize.removed();
		}

		SerializeProfiles profiles = Annotations.findAnnotation(SerializeProfiles.class, annotations);
		if (profiles != null) {
			if (!Arrays.asList(profiles.value()).contains(profile == null ? "" : profile)) {
				return null;
			}
			int addedProfile = getProfileVersion(profiles.value(), profiles.added());
			if (addedProfile != SerializeProfiles.DEFAULT_VERSION) {
				added = addedProfile;
			}
			int removedProfile = getProfileVersion(profiles.value(), profiles.removed());
			if (removedProfile != SerializeProfiles.DEFAULT_VERSION) {
				removed = removedProfile;
			}
		}

		if (serialize != null) {
			return new FoundSerializer(methodOrField, serialize.order(), added, removed, mods);
		}

		if (profiles != null || !mods.isEmpty()) {
			throw new IllegalArgumentException("Serialize modifiers without @Serialize annotation on " + methodOrField);
		}

		return null;
	}

	private int getProfileVersion(String[] profiles, int[] versions) {
		if (profiles == null || profiles.length == 0) {
			return SerializeProfiles.DEFAULT_VERSION;
		}
		for (int i = 0; i < profiles.length; i++) {
			if (Objects.equals(profile, profiles[i])) {
				if (i < versions.length) {
					return versions[i];
				}
				return SerializeProfiles.DEFAULT_VERSION;
			}
		}
		return SerializeProfiles.DEFAULT_VERSION;
	}

	@Nullable
	private FoundSerializer tryAddField(Class<?> classType, SerializerForType[] classGenerics, Field field) {
		FoundSerializer result = findAnnotations(field, field.getAnnotations());
		if (result == null) {
			return null;
		}
		check(isPublic(field.getModifiers()), "Field %s must be public", field);
		check(!isStatic(field.getModifiers()), "Field %s must not be static", field);
		check(!isTransient(field.getModifiers()), "Field %s must not be transient", field);
		result.serializerGen = resolveSerializer(classType, classGenerics, field.getGenericType(), result.mods).serializer;
		return result;
	}

	@Nullable
	private FoundSerializer tryAddGetter(Class<?> classType, SerializerForType[] classGenerics, Method getter) {
		if (getter.isBridge()) {
			return null;
		}
		FoundSerializer result = findAnnotations(getter, getter.getAnnotations());
		if (result == null) {
			return null;
		}
		check(isPublic(getter.getModifiers()), "Getter %s must be public", getter);
		check(!isStatic(getter.getModifiers()), "Getter %s must not be static", getter);
		check(getter.getReturnType() != Void.TYPE && getter.getParameterTypes().length == 0, "%s must be getter", getter);

		result.serializerGen = resolveSerializer(classType, classGenerics, getter.getGenericReturnType(), result.mods).serializer;

		return result;
	}

	private void scanAnnotations(Class<?> classType, SerializerForType[] classGenerics, SerializerGenClass serializerGenClass) {
		if (classType.isInterface()) {
			SerializeInterface annotation = Annotations.findAnnotation(SerializeInterface.class, classType.getAnnotations());
			scanInterface(classType, classGenerics, serializerGenClass, (annotation != null) && annotation.inherit());
			if (annotation != null) {
				Class<?> impl = annotation.impl();
				if (impl == void.class) {
					return;
				}
				scanSetters(impl, serializerGenClass);
				scanFactories(impl, serializerGenClass);
				scanConstructors(impl, serializerGenClass);
				serializerGenClass.addMatchingSetters();
			}
			return;
		}
		check(!classType.isAnonymousClass(), "Class should not be anonymous");
		check(!classType.isLocalClass(), "Class should not be local");
		scanClass(classType, classGenerics, serializerGenClass);
		scanFactories(classType, serializerGenClass);
		scanConstructors(classType, serializerGenClass);
		serializerGenClass.addMatchingSetters();
	}

	private void scanInterface(Class<?> classType, SerializerForType[] classGenerics, SerializerGenClass serializerGenClass, boolean inheritSerializers) {
		List<FoundSerializer> foundSerializers = new ArrayList<>();
		scanGetters(classType, classGenerics, foundSerializers);
		addMethodsAndGettersToClass(serializerGenClass, foundSerializers);
		if (!inheritSerializers) {
			return;
		}

		SerializeInterface annotation = Annotations.findAnnotation(SerializeInterface.class, classType.getAnnotations());
		if (annotation != null && !annotation.inherit()) {
			return;
		}
		for (Class<?> inter : classType.getInterfaces()) {
			scanInterface(inter, classGenerics, serializerGenClass, true);
		}
	}

	private void addMethodsAndGettersToClass(SerializerGenClass serializerGenClass, List<FoundSerializer> foundSerializers) {
		Set<Integer> orders = new HashSet<>();
		for (FoundSerializer foundSerializer : foundSerializers) {
			check(foundSerializer.order >= 0, "Invalid order %s for %s in %s", foundSerializer.order, foundSerializer,
					serializerGenClass.getRawType().getName());
			check(orders.add(foundSerializer.order), "Duplicate order %s for %s in %s", foundSerializer.order, foundSerializer,
					serializerGenClass.getRawType().getName());
		}
		Collections.sort(foundSerializers);
		for (FoundSerializer foundSerializer : foundSerializers) {
			if (foundSerializer.methodOrField instanceof Method) {
				serializerGenClass.addGetter((Method) foundSerializer.methodOrField, foundSerializer.serializerGen, foundSerializer.added, foundSerializer.removed);
			} else if (foundSerializer.methodOrField instanceof Field) {
				serializerGenClass.addField((Field) foundSerializer.methodOrField, foundSerializer.serializerGen, foundSerializer.added, foundSerializer.removed);
			} else {
				throw new AssertionError();
			}
		}
	}

	private void scanClass(Class<?> classType, SerializerForType[] classGenerics, SerializerGenClass serializerGenClass) {
		if (classType == Object.class) {
			return;
		}

		Type genericSuperclass = classType.getGenericSuperclass();
		if (genericSuperclass instanceof ParameterizedType) {
			ParameterizedType parameterizedSuperclass = (ParameterizedType) genericSuperclass;
			SerializerForType[] superclassGenerics = new SerializerForType[parameterizedSuperclass.getActualTypeArguments().length];
			for (int i = 0; i < parameterizedSuperclass.getActualTypeArguments().length; i++) {
				superclassGenerics[i] = resolveSerializer(classType, classGenerics,
						parameterizedSuperclass.getActualTypeArguments()[i], TypedModsMap.empty());
			}
			scanClass(classType.getSuperclass(), superclassGenerics, serializerGenClass);
		} else if (genericSuperclass instanceof Class) {
			scanClass(classType.getSuperclass(), new SerializerForType[]{}, serializerGenClass);
		} else {
			throw new IllegalArgumentException("Unsupported type " + genericSuperclass);
		}

		List<FoundSerializer> foundSerializers = scanSerializers(classType, classGenerics);
		addMethodsAndGettersToClass(serializerGenClass, foundSerializers);
		scanSetters(classType, serializerGenClass);
	}

	private List<FoundSerializer> scanSerializers(Class<?> classType, SerializerForType[] classGenerics) {
		List<FoundSerializer> foundSerializers = new ArrayList<>();
		scanFields(classType, classGenerics, foundSerializers);
		scanGetters(classType, classGenerics, foundSerializers);
		return foundSerializers;
	}

	private void scanFields(Class<?> classType, SerializerForType[] classGenerics, List<FoundSerializer> foundSerializers) {
		for (Field field : classType.getDeclaredFields()) {
			FoundSerializer foundSerializer = tryAddField(classType, classGenerics, field);
			if (foundSerializer != null) {
				foundSerializers.add(foundSerializer);
			}
		}
	}

	private void scanGetters(Class<?> classType, SerializerForType[] classGenerics, List<FoundSerializer> foundSerializers) {
		Method[] methods = classType.getDeclaredMethods();
		for (Method method : methods) {
			FoundSerializer foundSerializer = tryAddGetter(classType, classGenerics, method);
			if (foundSerializer != null) {
				foundSerializers.add(foundSerializer);
			}
		}
	}

	private void scanSetters(Class<?> classType, SerializerGenClass serializerGenClass) {
		for (Method method : classType.getDeclaredMethods()) {
			if (isStatic(method.getModifiers())) {
				continue;
			}
			if (method.getParameterTypes().length != 0) {
				List<String> fields = new ArrayList<>(method.getParameterTypes().length);
				for (int i = 0; i < method.getParameterTypes().length; i++) {
					Annotation[] parameterAnnotations = method.getParameterAnnotations()[i];
					Deserialize annotation = Annotations.findAnnotation(Deserialize.class, parameterAnnotations);
					if (annotation != null) {
						String field = annotation.value();
						fields.add(field);
					}
				}
				if (fields.size() == method.getParameterTypes().length) {
					serializerGenClass.addSetter(method, fields);
				} else {
					check(fields.isEmpty(), "Fields should not be empty");
				}
			}
		}
	}

	private void scanFactories(Class<?> classType, SerializerGenClass serializerGenClass) {
		DeserializeFactory annotationFactory = Annotations.findAnnotation(DeserializeFactory.class, classType.getAnnotations());
		Class<?> factoryClassType = (annotationFactory == null) ? classType : annotationFactory.value();
		for (Method factory : factoryClassType.getDeclaredMethods()) {
			if (classType != factory.getReturnType()) {
				continue;
			}
			if (factory.getParameterTypes().length != 0) {
				List<String> fields = new ArrayList<>(factory.getParameterTypes().length);
				for (int i = 0; i < factory.getParameterTypes().length; i++) {
					Annotation[] parameterAnnotations = factory.getParameterAnnotations()[i];
					Deserialize annotation = Annotations.findAnnotation(Deserialize.class, parameterAnnotations);
					if (annotation != null) {
						String field = annotation.value();
						fields.add(field);
					}
				}
				if (fields.size() == factory.getParameterTypes().length) {
					serializerGenClass.setFactory(factory, fields);
				} else {
					check(fields.isEmpty(), "@Deserialize is not fully specified for %s", fields);
				}
			}
		}
	}

	private void scanConstructors(Class<?> classType, SerializerGenClass serializerGenClass) {
		boolean found = false;
		for (Constructor<?> constructor : classType.getDeclaredConstructors()) {
			List<String> fields = new ArrayList<>(constructor.getParameterTypes().length);
			for (int i = 0; i < constructor.getParameterTypes().length; i++) {
				Annotation[] parameterAnnotations = constructor.getParameterAnnotations()[i];
				Deserialize annotation = Annotations.findAnnotation(Deserialize.class, parameterAnnotations);
				if (annotation != null) {
					String field = annotation.value();
					fields.add(field);
				}
			}
			if (constructor.getParameterTypes().length != 0 && fields.size() == constructor.getParameterTypes().length) {
				check(!found, "Duplicate @Deserialize constructor %s", constructor);
				found = true;
				serializerGenClass.setConstructor(constructor, fields);
			} else {
				check(fields.isEmpty(), "@Deserialize is not fully specified for %s", fields);
			}
		}
	}

	@SuppressWarnings("unchecked")
	private <T> BinarySerializer<T> buildBufferSerializer(SerializerGen serializerGen, int serializeVersion) {
		return (BinarySerializer<T>) createSerializer(serializerGen, serializeVersion);
	}

	/**
	 * Constructs buffer serializer for type, described by the given {@code SerializerGen}.
	 *
	 * @param serializerGen {@code SerializerGen} that describes the type that is to serialize
	 * @return buffer serializer for the given {@code SerializerGen}
	 */
	private <T> BinarySerializer<T> buildBufferSerializer(SerializerGen serializerGen) {
		return buildBufferSerializer(serializerGen, Integer.MAX_VALUE);
	}

	public class StaticMethods {
		private final DefiningClassLoader definingClassLoader = SerializerBuilder.this.definingClassLoader;

		public DefiningClassLoader getDefiningClassLoader() {
			return definingClassLoader;
		}

		private final class Key {
			public final SerializerGen serializerGen;
			public final int version;

			public Key(SerializerGen serializerGen, int version) {
				this.serializerGen = serializerGen;
				this.version = version;
			}

			@Override
			public boolean equals(Object o) {
				if (this == o) {
					return true;
				}
				if (o == null || getClass() != o.getClass()) {
					return false;
				}

				Key key = (Key) o;

				if (version != key.version) {
					return false;
				}
				return !(!Objects.equals(serializerGen, key.serializerGen));

			}

			@Override
			public int hashCode() {
				int result = serializerGen != null ? serializerGen.hashCode() : 0;
				result = 31 * result + version;
				return result;
			}
		}

		private final class Value {
			public String method;
			@Nullable
			public Expression expression;

			public Value(String method, @Nullable Expression expression) {
				this.method = method;
				this.expression = expression;
			}
		}

		private Map<Key, Value> mapSerialize = new HashMap<>();
		private Map<Key, Value> mapDeserialize = new HashMap<>();

		public boolean startSerializeStaticMethod(SerializerGen serializerGen, int version) {
			boolean b = mapSerialize.containsKey(new Key(serializerGen, version));
			if (!b) {
				String methodName = "serialize_" + serializerGen.getRawType().getSimpleName().replace('[', 's').replace(']', '_') + "_V" + version + "_" + counter.incrementAndGet();
				mapSerialize.put(new Key(serializerGen, version), new Value(methodName, null));
			}
			return b;
		}

		public boolean startDeserializeStaticMethod(SerializerGen serializerGen, int version) {
			boolean b = mapDeserialize.containsKey(new Key(serializerGen, version));
			if (!b) {
				String methodName = "deserialize_" + serializerGen.getRawType().getSimpleName().replace('[', 's').replace(']', '_') + "_V" + version + "_" + counter.incrementAndGet();
				mapDeserialize.put(new Key(serializerGen, version), new Value(methodName, null));
			}
			return b;
		}

		public void registerStaticSerializeMethod(SerializerGen serializerGen, int version, Expression expression) {
			Key key = new Key(serializerGen, version);
			Value value = mapSerialize.get(key);
			value.expression = expression;
		}

		public void registerStaticDeserializeMethod(SerializerGen serializerGen, int version, Expression expression) {
			Key key = new Key(serializerGen, version);
			Value value = mapDeserialize.get(key);
			value.expression = expression;
		}

		public Expression callStaticSerializeMethod(SerializerGen serializerGen, int version, Expression... args) {
			Value value = mapSerialize.get(new Key(serializerGen, version));
			return callStaticSelf(value.method, args);
		}

		public Expression callStaticDeserializeMethod(SerializerGen serializerGen, int version, Expression... args) {
			Value value = mapDeserialize.get(new Key(serializerGen, version));
			return callStaticSelf(value.method, args);
		}
	}

	synchronized private Object createSerializer(SerializerGen serializerGen, int serializeVersion) {
		ClassBuilder<BinarySerializer<?>> asmFactory = ClassBuilder.create(definingClassLoader, BinarySerializer.class);
		if (saveBytecodePath != null) {
			asmFactory.withBytecodeSaveDir(saveBytecodePath);
		}

		Preconditions.check(serializeVersion >= 0, "serializerVersion is negative");
		Class<?> dataType = serializerGen.getRawType();

		List<Integer> versions = new ArrayList<>();
		List<Integer> allVersions = new ArrayList<>();
		for (int v : VersionsCollector.versions(serializerGen)) {
			if (v <= serializeVersion) {
				versions.add(v);
			}
			allVersions.add(v);
		}
		Collections.sort(versions);
		Collections.sort(allVersions);
		Integer currentVersion = !allVersions.isEmpty() && versions.isEmpty() ?
				Integer.valueOf(serializeVersion) :
				getLatestVersion(versions);

		StaticMethods staticMethods = new StaticMethods();

		serializerGen.prepareSerializeStaticMethods(currentVersion != null ? currentVersion : 0,
				staticMethods, compatibilityLevel);
		for (StaticMethods.Key key : staticMethods.mapSerialize.keySet()) {
			StaticMethods.Value value = staticMethods.mapSerialize.get(key);
			asmFactory.withStaticMethod(value.method,
					int.class,
					asList(byte[].class, int.class, key.serializerGen.getRawType()),
					value.expression);
		}
		asmFactory.withMethod("encode", int.class, asList(byte[].class, int.class, Object.class),
				let(currentVersion == null ?
								arg(1) :
								callStatic(BinaryOutputUtils.class, "writeVarInt", arg(0), arg(1), value(currentVersion)),
						pos ->
								serializerGen.serialize(
										arg(0),
										pos,
										cast(arg(2), dataType),
										currentVersion != null ? currentVersion : 0, staticMethods, compatibilityLevel))
		);

		defineDeserialize(serializerGen, asmFactory, allVersions, staticMethods);
		for (StaticMethods.Key key : staticMethods.mapDeserialize.keySet()) {
			StaticMethods.Value value = staticMethods.mapDeserialize.get(key);
			asmFactory.withStaticMethod(value.method,
					key.serializerGen.getRawType(),
					asList(BinaryInput.class),
					value.expression);
		}

		asmFactory.withMethod("encode", void.class, asList(BinaryOutput.class, Object.class),
				let(call(self(), "encode",
						call(arg(0), "array"),
						call(arg(0), "pos"),
						arg(1)),
						newPos -> call(arg(0), "pos", newPos)));

		asmFactory.withMethod("decode", Object.class, asList(byte[].class, int.class),
				call(self(), "decode", constructor(BinaryInput.class, arg(0), arg(1))));

		return asmFactory.buildClassAndCreateNewInstance();
	}

	private void defineDeserialize(SerializerGen serializerGen,
			ClassBuilder<BinarySerializer<?>> asmFactory,
			List<Integer> allVersions,
			StaticMethods staticMethods) {
		defineDeserializeLatest(serializerGen, asmFactory, getLatestVersion(allVersions), staticMethods);

		defineDeserializeEarlierVersion(serializerGen, asmFactory, allVersions, staticMethods);
		for (int i = allVersions.size() - 2; i >= 0; i--) {
			int version = allVersions.get(i);
			defineDeserializeVersion(serializerGen, asmFactory, version, staticMethods);
		}
	}

	private void defineDeserializeLatest(SerializerGen serializerGen,
			ClassBuilder<BinarySerializer<?>> asmFactory,
			Integer latestVersion,
			StaticMethods staticMethods) {
		if (latestVersion == null) {
			serializerGen.prepareDeserializeStaticMethods(0, staticMethods, compatibilityLevel);
			asmFactory.withMethod("decode", Object.class, asList(BinaryInput.class),
					serializerGen.deserialize(serializerGen.getRawType(), 0, staticMethods, compatibilityLevel));
		} else {
			serializerGen.prepareDeserializeStaticMethods(latestVersion, staticMethods, compatibilityLevel);
			asmFactory.withMethod("decode", Object.class, asList(BinaryInput.class),
					let(call(arg(0), "readVarInt"),
							version -> ifThenElse(cmpEq(version, value(latestVersion)),
									serializerGen.deserialize(serializerGen.getRawType(), latestVersion, staticMethods, compatibilityLevel),
									call(self(), "deserializeEarlierVersions", arg(0), version))));
		}
	}

	private void defineDeserializeEarlierVersion(SerializerGen serializerGen,
			ClassBuilder<BinarySerializer<?>> asmFactory,
			List<Integer> allVersions,
			StaticMethods staticMethods) {
		List<Expression> listKey = new ArrayList<>();
		List<Expression> listValue = new ArrayList<>();
		for (int i = allVersions.size() - 2; i >= 0; i--) {
			int version = allVersions.get(i);
			listKey.add(value(version));
			serializerGen.prepareDeserializeStaticMethods(version, staticMethods, compatibilityLevel);
			listValue.add(call(self(), "deserializeVersion" + version, arg(0)));
		}
		asmFactory.withMethod("deserializeEarlierVersions", serializerGen.getRawType(), asList(BinaryInput.class, int.class),
				switchForKey(arg(1), listKey, listValue));
	}

	private void defineDeserializeVersion(SerializerGen serializerGen,
			ClassBuilder<BinarySerializer<?>> asmFactory,
			int version, StaticMethods staticMethods) {
		asmFactory.withMethod("deserializeVersion" + version,
				serializerGen.getRawType(),
				asList(BinaryInput.class),
				sequence(serializerGen.deserialize(serializerGen.getRawType(), version, staticMethods, compatibilityLevel)));
	}

	@Nullable
	private Integer getLatestVersion(List<Integer> versions) {
		return versions.isEmpty() ? null : versions.get(versions.size() - 1);
	}

	private static final class Key {
		final Class<?> type;
		final SerializerForType[] generics;
		final List<SerializerGenBuilder> mods;

		private Key(Class<?> type, SerializerForType[] generics, List<SerializerGenBuilder> mods) {
			this.type = checkNotNull(type);
			this.generics = checkNotNull(generics);
			this.mods = checkNotNull(mods);
		}

		@Override
		public boolean equals(Object o) {
			if (this == o) {
				return true;
			}
			if (o == null || getClass() != o.getClass()) {
				return false;
			}

			Key key = (Key) o;

			if (!Arrays.equals(generics, key.generics)) {
				return false;
			}
			if (!type.equals(key.type)) {
				return false;
			}
			return mods.equals(key.mods);
		}

		@Override
		public int hashCode() {
			int result = type.hashCode();
			result = 31 * result + Arrays.hashCode(generics);
			result = 31 * result + mods.hashCode();
			return result;
		}
	}

}
