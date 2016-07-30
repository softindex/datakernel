/*
 * Copyright (C) 2015 SoftIndex LLC.
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
import io.datakernel.codegen.AsmBuilder;
import io.datakernel.codegen.Expression;
import io.datakernel.codegen.Variable;
import io.datakernel.codegen.utils.DefiningClassLoader;
import io.datakernel.codegen.utils.Preconditions;
import io.datakernel.serializer.annotations.*;
import io.datakernel.serializer.asm.*;

import java.lang.annotation.Annotation;
import java.lang.reflect.*;
import java.net.Inet4Address;
import java.net.Inet6Address;
import java.net.InetAddress;
import java.nio.file.Path;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;

import static io.datakernel.codegen.Expressions.*;
import static io.datakernel.codegen.utils.Preconditions.check;
import static io.datakernel.codegen.utils.Preconditions.checkNotNull;
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
	private StringFormat defaultFormat = StringFormat.UTF8;
	private Path saveBytecodePath;
	private CompatibilityLevel compatibilityLevel = CompatibilityLevel.LEVEL_3;

	private final Map<Class<?>, SerializerGenBuilder> typeMap = new LinkedHashMap<>();

	private final Map<Class<? extends Annotation>, Class<? extends Annotation>> annotationsExMap = new LinkedHashMap<>();
	private final Map<Class<? extends Annotation>, AnnotationHandler<?, ?>> annotationsMap = new LinkedHashMap<>();
	private final Map<String, Collection<Class<?>>> extraSubclassesMap = new HashMap<>();

	private SerializerBuilder(DefiningClassLoader definingClassLoader) {
		this.definingClassLoader = definingClassLoader;
	}

	public static SerializerBuilder newDefaultInstance(ClassLoader classLoader) {
		return newDefaultInstance(new DefiningClassLoader(classLoader));
	}

	public static SerializerBuilder newDefaultInstance(String profile, ClassLoader classLoader) {
		return newDefaultInstance(new DefiningClassLoader(classLoader)).setProfile(profile);
	}

	public static SerializerBuilder newDefaultInstance(String profile, DefiningClassLoader definingClassLoader) {
		return newDefaultInstance(definingClassLoader).setProfile(profile);
	}

	public SerializerBuilder compatibilityLevel(CompatibilityLevel compatibilityLevel) {
		this.compatibilityLevel = compatibilityLevel;
		return this;
	}

	public SerializerBuilder setSaveBytecodePath(Path path) {
		this.saveBytecodePath = path;
		return this;
	}

	public SerializerBuilder version(int version) {
		this.version = version;
		return this;
	}

	public SerializerBuilder defaultStringFormat(StringFormat format) {
		this.defaultFormat = format;
		this.register(String.class, new SerializerGenString(format));
		return this;
	}

	/**
	 * Constructs a {@code SerializerBuilder} that uses default settings.
	 *
	 * @return default serializer builder
	 */
	public static SerializerBuilder newDefaultInstance(DefiningClassLoader definingClassLoader) {
		final SerializerBuilder result = new SerializerBuilder(definingClassLoader);

		result.register(Object.class, new SerializerGenBuilder() {
			@Override
			public SerializerGen serializer(final Class<?> type, final SerializerForType[] generics, SerializerGen fallback) {
				check(type.getTypeParameters().length == generics.length);
				check(fallback == null);
				final SerializerGenClass serializer;
				SerializeInterface annotation = Annotations.findAnnotation(SerializeInterface.class, type.getAnnotations());
				if (annotation != null && annotation.impl() != void.class) {
					serializer = new SerializerGenClass(type, generics, annotation.impl());
				} else {
					serializer = new SerializerGenClass(type, generics);
				}
				result.initTasks.add(new Runnable() {
					@Override
					public void run() {
						result.scanAnnotations(type, generics, serializer);
					}
				});
				return serializer;
			}
		});
		result.register(List.class, new SerializerGenBuilder() {
			@Override
			public SerializerGen serializer(Class<?> type, final SerializerForType[] generics, SerializerGen fallback) {
				check(generics.length == 1);
				return new SerializerGenList(generics[0].serializer);
			}
		});
		result.register(Set.class, new SerializerGenBuilder() {
			@Override
			public SerializerGen serializer(Class<?> type, SerializerForType[] generics, SerializerGen fallback) {
				check(generics.length == 1);
				return new SerializerGenSet(generics[0].serializer);
			}
		});
		result.register(Map.class, new SerializerGenBuilder() {
			@Override
			public SerializerGen serializer(Class<?> type, final SerializerForType[] generics, SerializerGen fallback) {
				check(generics.length == 2);
				return new SerializerGenMap(generics[0].serializer, generics[1].serializer);
			}
		});
		result.register(Enum.class, new SerializerGenBuilder() {
			@Override
			public SerializerGen serializer(final Class<?> type, final SerializerForType[] generics, SerializerGen fallback) {
				List<FoundSerializer> foundSerializers = result.scanSerializers(type, generics);
				if (!foundSerializers.isEmpty()) {
					final SerializerGenClass serializer = new SerializerGenClass(type);
					result.initTasks.add(new Runnable() {
						@Override
						public void run() {
							result.scanAnnotations(type, generics, serializer);
						}
					});
					return serializer;
				} else {
					return new SerializerGenEnum(type);
				}
			}
		});
		result.register(Boolean.TYPE, new SerializerGenBoolean());
		result.register(Character.TYPE, new SerializerGenChar());
		result.register(Byte.TYPE, new SerializerGenByte());
		result.register(Short.TYPE, new SerializerGenShort());
		result.register(Integer.TYPE, new SerializerGenInt(false));
		result.register(Long.TYPE, new SerializerGenLong(false));
		result.register(Float.TYPE, new SerializerGenFloat());
		result.register(Double.TYPE, new SerializerGenDouble());
		result.register(Boolean.class, new SerializerGenBoolean());
		result.register(Character.class, new SerializerGenChar());
		result.register(Byte.class, new SerializerGenByte());
		result.register(Short.class, new SerializerGenShort());
		result.register(Integer.class, new SerializerGenInt(false));
		result.register(Long.class, new SerializerGenLong(false));
		result.register(Float.class, new SerializerGenFloat());
		result.register(Double.class, new SerializerGenDouble());
		result.register(String.class, new SerializerGenString());
		result.register(Inet4Address.class, SerializerGenInet4Address.instance());
		result.register(Inet6Address.class, SerializerGenInet6Address.instance());

		LinkedHashMap<Class<?>, SerializerGen> addressMap = new LinkedHashMap<>();
		addressMap.put(Inet4Address.class, SerializerGenInet4Address.instance());
		addressMap.put(Inet6Address.class, SerializerGenInet6Address.instance());
		result.register(InetAddress.class, new SerializerGenSubclass(InetAddress.class, addressMap));

		result.register(SerializerClass.class, SerializerClassEx.class, new SerializerClassHandler());
		result.register(SerializeFixedSize.class, SerializeFixedSizeEx.class, new SerializeFixedSizeHandler());
		result.register(SerializeVarLength.class, SerializeVarLengthEx.class, new SerializeVarLengthHandler());
		result.register(SerializeSubclasses.class, SerializeSubclassesEx.class, new SerializeSubclassesHandler());
		result.register(SerializeNullable.class, SerializeNullableEx.class, new SerializeNullableHandler());
		result.register(SerializeMaxLength.class, SerializeMaxLengthEx.class, new SerializeMaxLengthHandler());
		result.register(SerializeStringFormat.class, SerializeStringFormatEx.class, new SerializeStringFormatHandler());
		return result;
	}

	public static <T> BufferSerializer<T> newDefaultSerializer(Class<T> type, ClassLoader classLoader) {
		return newDefaultInstance(new DefiningClassLoader(classLoader)).create(type);
	}

	public static <T> BufferSerializer<T> newDefaultSerializer(Class<T> type, DefiningClassLoader definingClassLoader) {
		return newDefaultInstance(definingClassLoader).create(type);
	}

	public SerializerBuilder setProfile(String profile) {
		this.profile = profile;
		return this;
	}

	public <A extends Annotation, P extends Annotation> SerializerBuilder register(Class<A> annotation,
	                                                                               Class<P> annotationPlural,
	                                                                               AnnotationHandler<A, P> annotationHandler) {
		annotationsMap.put(annotation, annotationHandler);
		if (annotationPlural != null)
			annotationsExMap.put(annotation, annotationPlural);
		return this;
	}

	public SerializerBuilder register(Class<?> type, SerializerGenBuilder serializer) {
		typeMap.put(type, serializer);
		return this;
	}

	public SerializerBuilder register(Class<?> type, final SerializerGen serializer) {
		return register(type, new SerializerGenBuilderConst(serializer));
	}

	public SerializerBuilder setExtraSubclasses(String extraSubclassesId, Collection<Class<?>> subclasses) {
		extraSubclassesMap.put(extraSubclassesId, subclasses);
		return this;
	}

	public SerializerBuilder setExtraSubclasses(String extraSubclassesId, Class<?>... subclasses) {
		return setExtraSubclasses(extraSubclassesId, Arrays.asList(subclasses));
	}

	/**
	 * Creates a {@code SerializerGen} for the given type token.
	 *
	 * @return {@code SerializerGen} for the given type token
	 */
	public <T> BufferSerializer<T> create(Class<T> type) {
		SerializerGenBuilder.SerializerForType[] serializerForTypes = new SerializerGenBuilder.SerializerForType[0];
		return create(type, serializerForTypes);
	}

	public <T> BufferSerializer<T> create(SerializerGen serializerGen) {
		return createBufferSerializer(serializerGen, version);
	}

	public StringFormat getDefaultFormat() {
		return defaultFormat;
	}

	private static final class Key {
		final Class<?> type;
		final SerializerGenBuilder.SerializerForType[] generics;
		final List<SerializerGenBuilder> mods;

		private Key(Class<?> type, SerializerGenBuilder.SerializerForType[] generics, List<SerializerGenBuilder> mods) {
			this.type = checkNotNull(type);
			this.generics = checkNotNull(generics);
			this.mods = checkNotNull(mods);
		}

		@Override
		public boolean equals(Object o) {
			if (this == o) return true;
			if (o == null || getClass() != o.getClass()) return false;

			Key key = (Key) o;

			if (!Arrays.equals(generics, key.generics)) return false;
			if (!type.equals(key.type)) return false;
			if (!mods.equals(key.mods)) return false;

			return true;
		}

		@Override
		public int hashCode() {
			int result = type.hashCode();
			result = 31 * result + Arrays.hashCode(generics);
			result = 31 * result + mods.hashCode();
			return result;
		}
	}

	private final Map<Key, SerializerGen> cachedSerializers = new HashMap<>();
	private final List<Runnable> initTasks = new ArrayList<>();

	public <T> BufferSerializer<T> create(Class<?> type, SerializerGenBuilder.SerializerForType[] generics) {
		return createBufferSerializer(create(type, generics, Collections.<SerializerGenBuilder>emptyList()), version);
	}

	private SerializerGen create(Class<?> type, SerializerGenBuilder.SerializerForType[] generics, List<SerializerGenBuilder> mods) {
		Key key = new Key(type, generics, mods);
		SerializerGen serializer = cachedSerializers.get(key);
		if (serializer == null) {
			serializer = createSerializer(type, generics, mods);
			cachedSerializers.put(key, serializer);
		}
		while (!initTasks.isEmpty()) {
			initTasks.remove(0).run();
		}
		return serializer;
	}

	private SerializerGen createSerializer(Class<?> type, SerializerGenBuilder.SerializerForType[] generics, List<SerializerGenBuilder> mods) {
		if (!mods.isEmpty()) {
			SerializerGen serializer = create(type, generics, mods.subList(0, mods.size() - 1));
			SerializerGenBuilder last = mods.get(mods.size() - 1);
			return last.serializer(type, generics, serializer);
		}

		if (type.isArray()) {
			check(generics.length == 1);
			SerializerGen itemSerializer = generics[0].serializer;
			return new SerializerGenArray(itemSerializer, type);
		}

		SerializeSubclasses serializeSubclasses = Annotations.findAnnotation(SerializeSubclasses.class, type.getAnnotations());
		if (serializeSubclasses != null) {
			return createSubclassesSerializer(type, serializeSubclasses);
		}

		Class<?> key = findKey(type, typeMap.keySet());
		final SerializerGenBuilder builder = typeMap.get(key);
		if (builder == null)
			throw new IllegalArgumentException();
		SerializerGen serializer = builder.serializer(type, generics, null);
		checkNotNull(serializer);
		return serializer;
	}

	public SerializerGen createSubclassesSerializer(Class<?> type, SerializeSubclasses serializeSubclasses) {
		LinkedHashSet<Class<?>> subclassesSet = new LinkedHashSet<>();
		subclassesSet.addAll(Arrays.asList(serializeSubclasses.value()));
		check(subclassesSet.size() == serializeSubclasses.value().length);

		if (!serializeSubclasses.extraSubclassesId().isEmpty()) {
			Collection<Class<?>> registeredSubclasses = extraSubclassesMap.get(serializeSubclasses.extraSubclassesId());
			if (registeredSubclasses != null)
				subclassesSet.addAll(registeredSubclasses);
		}
		return createSubclassesSerializer(type, subclassesSet);
	}

	public SerializerGen createSubclassesSerializer(Class<?> type, Collection<Class<?>> subclasses) {
		LinkedHashSet<Class<?>> subclassesSet = new LinkedHashSet<>();
		subclassesSet.addAll(subclasses);
		check(subclassesSet.size() == subclasses.size());
		return createSubclassesSerializer(type, subclassesSet);
	}

	private SerializerGen createSubclassesSerializer(Class<?> type, LinkedHashSet<Class<?>> subclassesSet) {
		checkNotNull(subclassesSet);
		check(!subclassesSet.isEmpty());
		LinkedHashMap<Class<?>, SerializerGen> subclasses = new LinkedHashMap<>();
		for (Class<?> subclass : subclassesSet) {
			check(subclass.getTypeParameters().length == 0);
			check(type.isAssignableFrom(subclass), "Unrelated subclass '%s' for '%s'", subclass, type);

			SerializerGen serializer = create(
					subclass,
					new SerializerGenBuilder.SerializerForType[]{},
					Collections.<SerializerGenBuilder>emptyList()
			);
			subclasses.put(subclass, serializer);
		}
		return new SerializerGenSubclass(type, subclasses);
	}

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
		TypedModsMap.Builder rootBuilder = TypedModsMap.builder();
		if (annotations.length == 0)
			return rootBuilder.build();
		for (Class<? extends Annotation> annotationType : annotationsMap.keySet()) {
			Class<? extends Annotation> annotationExType = annotationsExMap.get(annotationType);
			AnnotationHandler annotationHandler = annotationsMap.get(annotationType);
			for (Annotation annotation : annotations) {
				if (annotation.annotationType() == annotationType) {
					SerializerGenBuilder serializerGenBuilder = annotationHandler.createBuilder(this, annotation, compatibilityLevel);
					TypedModsMap.Builder child = rootBuilder.ensureChild(annotationHandler.extractPath(annotation));
					child.add(serializerGenBuilder);
				}
			}
			for (Annotation annotationEx : annotations) {
				if (annotationEx.annotationType() == annotationExType) {
					for (Annotation annotation : annotationHandler.extractList(annotationEx)) {
						SerializerGenBuilder serializerGenBuilder = annotationHandler.createBuilder(this, annotation, compatibilityLevel);
						TypedModsMap.Builder child = rootBuilder.ensureChild(annotationHandler.extractPath(annotation));
						child.add(serializerGenBuilder);
					}
				}
			}
		}
		return rootBuilder.build();
	}

	@SuppressWarnings("rawtypes")
	public SerializerGenBuilder.SerializerForType resolveSerializer(Class<?> classType, SerializerGenBuilder.SerializerForType[] classGenerics, Type genericType, TypedModsMap typedModsMap) {
		if (genericType instanceof TypeVariable) {
			String typeVariableName = ((TypeVariable) genericType).getName();

			int i;
			for (i = 0; i < classType.getTypeParameters().length; i++) {
				TypeVariable<?> typeVariable = classType.getTypeParameters()[i];
				if (typeVariableName.equals(typeVariable.getName())) {
					break;
				}
			}
			check(i < classType.getTypeParameters().length);

			SerializerGen serializer = typedModsMap.rewrite(classGenerics[i].rawType, new SerializerGenBuilder.SerializerForType[]{}, classGenerics[i].serializer);
			return new SerializerGenBuilder.SerializerForType(classGenerics[i].rawType, serializer);
		} else if (genericType instanceof ParameterizedType) {
			ParameterizedType parameterizedType = (ParameterizedType) genericType;

			SerializerGenBuilder.SerializerForType[] typeArguments = new SerializerGenBuilder.SerializerForType[parameterizedType.getActualTypeArguments().length];
			for (int i = 0; i < parameterizedType.getActualTypeArguments().length; i++) {
				Type typeArgument = parameterizedType.getActualTypeArguments()[i];
				if (typeArgument instanceof WildcardType) {
					throw new IllegalArgumentException();
				}
				typeArguments[i] = resolveSerializer(classType, classGenerics, typeArgument, typedModsMap.get(i));
			}

			Class<?> rawType = (Class<?>) parameterizedType.getRawType();
			SerializerGen serializer = create(rawType, typeArguments, typedModsMap.getMods());
			return new SerializerGenBuilder.SerializerForType(rawType, serializer);
		} else if (genericType instanceof GenericArrayType) {
			throw new UnsupportedOperationException();
		} else if (genericType instanceof Class<?>) {
			Class<?> rawType = (Class<?>) genericType;
			SerializerGenBuilder.SerializerForType[] generics = {};
			if (rawType.isArray()) {
				Class<?> componentType = rawType.getComponentType();
				SerializerGenBuilder.SerializerForType forType = resolveSerializer(classType, classGenerics, componentType, typedModsMap.get(0));
				generics = new SerializerGenBuilder.SerializerForType[]{forType};
			}
			SerializerGen serializer = create(rawType, generics, typedModsMap.getMods());
			return new SerializerGenBuilder.SerializerForType(rawType, serializer);
		} else {
			throw new IllegalArgumentException();
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
			if (methodOrField instanceof Field)
				return ((Field) methodOrField).getName();
			if (methodOrField instanceof Method)
				return ((Method) methodOrField).getName();
			throw new AssertionError();
		}

		private int fieldRank() {
			if (methodOrField instanceof Field)
				return 1;
			if (methodOrField instanceof Method)
				return 2;
			throw new AssertionError();
		}

		@SuppressWarnings("NullableProblems")
		@Override
		public int compareTo(FoundSerializer o) {
			int result = Integer.compare(this.order, o.order);
			if (result != 0)
				return result;
			result = Integer.compare(fieldRank(), o.fieldRank());
			if (result != 0)
				return result;
			result = getName().compareTo(o.getName());
			if (result != 0)
				return result;
			return 0;
		}

		@Override
		public String toString() {
			return methodOrField.getClass().getSimpleName() + " " + getName();
		}
	}

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
			if (!Arrays.asList(profiles.value()).contains((profile == null ? "" : profile)))
				return null;
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

		if (profiles != null || !mods.isEmpty())
			throw new IllegalArgumentException("Serialize modifiers without @Serialize annotation on " + methodOrField);

		return null;
	}

	private int getProfileVersion(String[] profiles, int[] versions) {
		if (profiles == null || profiles.length == 0) return SerializeProfiles.DEFAULT_VERSION;
		for (int i = 0; i < profiles.length; i++) {
			if (Objects.equals(profile, profiles[i])) {
				if (i < versions.length)
					return versions[i];
				return SerializeProfiles.DEFAULT_VERSION;
			}
		}
		return SerializeProfiles.DEFAULT_VERSION;
	}

	private FoundSerializer tryAddField(Class<?> classType, SerializerGenBuilder.SerializerForType[] classGenerics, Field field) {
		FoundSerializer result = findAnnotations(field, field.getAnnotations());
		if (result == null)
			return null;
		check(isPublic(field.getModifiers()), "Field %s must be public", field);
		check(!isStatic(field.getModifiers()), "Field %s must not be static", field);
		check(!isTransient(field.getModifiers()), "Field %s must not be transient", field);
		result.serializerGen = resolveSerializer(classType, classGenerics, field.getGenericType(), result.mods).serializer;
		return result;
	}

	private FoundSerializer tryAddGetter(Class<?> classType, SerializerGenBuilder.SerializerForType[] classGenerics, Method getter) {
		FoundSerializer result = findAnnotations(getter, getter.getAnnotations());
		if (result == null)
			return null;
		check(isPublic(getter.getModifiers()), "Getter %s must be public", getter);
		check(!isStatic(getter.getModifiers()), "Getter %s must not be static", getter);
		check(getter.getReturnType() != Void.TYPE && getter.getParameterTypes().length == 0, "%s must be getter", getter);

		result.serializerGen = resolveSerializer(classType, classGenerics, getter.getGenericReturnType(), result.mods).serializer;

		return result;
	}

	private void scanAnnotations(Class<?> classType, SerializerGenBuilder.SerializerForType[] classGenerics, SerializerGenClass serializerGenClass) {
		if (classType.isInterface()) {
			SerializeInterface annotation = Annotations.findAnnotation(SerializeInterface.class, classType.getAnnotations());
			scanInterface(classType, classGenerics, serializerGenClass, (annotation != null) && annotation.inherit());
			if (annotation != null) {
				Class<?> impl = annotation.impl();
				if (impl == void.class)
					return;
				scanSetters(impl, serializerGenClass);
				scanFactories(impl, serializerGenClass);
				scanConstructors(impl, serializerGenClass);
				serializerGenClass.addMatchingSetters();
			}
			return;
		}
		check(!classType.isAnonymousClass());
		check(!classType.isLocalClass());
		scanClass(classType, classGenerics, serializerGenClass);
		scanFactories(classType, serializerGenClass);
		scanConstructors(classType, serializerGenClass);
		serializerGenClass.addMatchingSetters();
	}

	private void scanInterface(Class<?> classType, SerializerGenBuilder.SerializerForType[] classGenerics, SerializerGenClass serializerGenClass, boolean inheritSerializers) {
		List<FoundSerializer> foundSerializers = new ArrayList<>();
		scanGetters(classType, classGenerics, foundSerializers);
		addMethodsAndGettersToClass(serializerGenClass, foundSerializers);
		if (!inheritSerializers)
			return;

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
			if (foundSerializer.methodOrField instanceof Method)
				serializerGenClass.addGetter((Method) foundSerializer.methodOrField, foundSerializer.serializerGen, foundSerializer.added, foundSerializer.removed);
			else if (foundSerializer.methodOrField instanceof Field)
				serializerGenClass.addField((Field) foundSerializer.methodOrField, foundSerializer.serializerGen, foundSerializer.added, foundSerializer.removed);
			else
				throw new AssertionError();
		}
	}

	private void scanClass(Class<?> classType, SerializerGenBuilder.SerializerForType[] classGenerics, SerializerGenClass serializerGenClass) {
		if (classType == Object.class)
			return;

		Type genericSuperclass = classType.getGenericSuperclass();
		if (genericSuperclass instanceof ParameterizedType) {
			ParameterizedType parameterizedSuperclass = (ParameterizedType) genericSuperclass;
			SerializerGenBuilder.SerializerForType[] superclassGenerics = new SerializerGenBuilder.SerializerForType[parameterizedSuperclass.getActualTypeArguments().length];
			for (int i = 0; i < parameterizedSuperclass.getActualTypeArguments().length; i++) {
				superclassGenerics[i] = resolveSerializer(classType, classGenerics,
						parameterizedSuperclass.getActualTypeArguments()[i], TypedModsMap.empty());
			}
			scanClass(classType.getSuperclass(), superclassGenerics, serializerGenClass);
		} else if (genericSuperclass instanceof Class) {
			scanClass(classType.getSuperclass(), new SerializerGenBuilder.SerializerForType[]{}, serializerGenClass);
		} else
			throw new IllegalArgumentException();

		List<FoundSerializer> foundSerializers = scanSerializers(classType, classGenerics);
		addMethodsAndGettersToClass(serializerGenClass, foundSerializers);
		scanSetters(classType, serializerGenClass);
	}

	private List<FoundSerializer> scanSerializers(Class<?> classType, SerializerGenBuilder.SerializerForType[] classGenerics) {
		List<FoundSerializer> foundSerializers = new ArrayList<>();
		scanFields(classType, classGenerics, foundSerializers);
		scanGetters(classType, classGenerics, foundSerializers);
		return foundSerializers;
	}

	private void scanFields(Class<?> classType, SerializerGenBuilder.SerializerForType[] classGenerics, List<FoundSerializer> foundSerializers) {
		for (Field field : classType.getDeclaredFields()) {
			FoundSerializer foundSerializer = tryAddField(classType, classGenerics, field);
			if (foundSerializer != null)
				foundSerializers.add(foundSerializer);
		}
	}

	private void scanGetters(Class<?> classType, SerializerGenBuilder.SerializerForType[] classGenerics, List<FoundSerializer> foundSerializers) {
		Method[] methods = classType.getDeclaredMethods();
		for (Method method : methods) {
			FoundSerializer foundSerializer = tryAddGetter(classType, classGenerics, method);
			if (foundSerializer != null)
				foundSerializers.add(foundSerializer);
		}
	}

	private void scanSetters(Class<?> classType, SerializerGenClass serializerGenClass) {
		for (Method method : classType.getDeclaredMethods()) {
			if (isStatic(method.getModifiers()))
				continue;
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
					check(fields.isEmpty());
				}
			}
		}
	}

	private void scanFactories(Class<?> classType, SerializerGenClass serializerGenClass) {
		DeserializeFactory annotationFactory = Annotations.findAnnotation(DeserializeFactory.class, classType.getAnnotations());
		Class<?> factoryClassType = (annotationFactory == null) ? classType : annotationFactory.value();
		for (Method factory : factoryClassType.getDeclaredMethods()) {
			if (classType != factory.getReturnType())
				continue;
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

	private <T> BufferSerializer<T> createBufferSerializer(SerializerGen serializerGen, int serializeVersion) {
		return (BufferSerializer<T>) createSerializer(serializerGen, serializeVersion);
	}

	/**
	 * Constructs buffer serializer for type, described by the given {@code SerializerGen}.
	 *
	 * @param serializerGen {@code SerializerGen} that describes the type that is to serialize
	 * @return buffer serializer for the given {@code SerializerGen}
	 */
	private <T> BufferSerializer<T> createBufferSerializer(SerializerGen serializerGen) {
		return createBufferSerializer(serializerGen, Integer.MAX_VALUE);
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
				if (this == o) return true;
				if (o == null || getClass() != o.getClass()) return false;

				Key key = (Key) o;

				if (version != key.version) return false;
				return !(serializerGen != null ? !serializerGen.equals(key.serializerGen) : key.serializerGen != null);

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
			public Expression expression;

			public Value(String method, Expression expression) {
				this.method = method;
				this.expression = expression;
			}
		}

		private Map<Key, Value> mapSerialize = new HashMap<>();
		private Map<Key, Value> mapDeserialize = new HashMap<>();

		public boolean startSerializeStaticMethod(SerializerGen serializerGen, int version) {
			boolean b = mapSerialize.containsKey(new Key(serializerGen, version));
			if (!b) {
				String methodName = "serialize_" + serializerGen.getRawType().getSimpleName().replace('[', 's').replace(']', '_') + "_V" + version + "_" + (counter.incrementAndGet());
				mapSerialize.put(new Key(serializerGen, version), new Value(methodName, null));
			}
			return b;
		}

		public boolean startDeserializeStaticMethod(SerializerGen serializerGen, int version) {
			boolean b = mapDeserialize.containsKey(new Key(serializerGen, version));
			if (!b) {
				String methodName = "deserialize_" + serializerGen.getRawType().getSimpleName().replace('[', 's').replace(']', '_') + "_V" + version + "_" + (counter.incrementAndGet());
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
		AsmBuilder<BufferSerializer> asmFactory = new AsmBuilder<>(definingClassLoader, BufferSerializer.class);
		if (saveBytecodePath != null) {
			asmFactory.setBytecodeSaveDir(saveBytecodePath);
		}

		Preconditions.check(serializeVersion >= 0, "serializerVersion is negative");
		Class<?> dataType = serializerGen.getRawType();

		List<Integer> versions = new ArrayList<>();
		List<Integer> allVersions = new ArrayList<>();
		for (int v : SerializerGen.VersionsCollector.versions(serializerGen)) {
			if (v <= serializeVersion)
				versions.add(v);
			allVersions.add(v);
		}
		Collections.sort(versions);
		Collections.sort(allVersions);
		Integer currentVersion = getLatestVersion(versions);
		if (!allVersions.isEmpty() && currentVersion == null)
			currentVersion = serializeVersion;

		Expression version = voidExp();
		if (currentVersion != null) {
			version = call(arg(0), "position",
					callStatic(SerializerUtils.class, "writeVarInt", call(arg(0), "array"), call(arg(0), "position"), value(currentVersion)));
		}

		StaticMethods staticMethods = new StaticMethods();

		if (currentVersion == null) currentVersion = 0;
		serializerGen.prepareSerializeStaticMethods(currentVersion, staticMethods, compatibilityLevel);
		for (StaticMethods.Key key : staticMethods.mapSerialize.keySet()) {
			StaticMethods.Value value = staticMethods.mapSerialize.get(key);
			asmFactory.staticMethod(value.method,
					int.class,
					asList(byte[].class, int.class, key.serializerGen.getRawType()),
					value.expression);
		}
		Variable position = let(call(arg(0), "position"));
		asmFactory.method("serialize", sequence(version,
						call(arg(0), "position", serializerGen.serialize(
								call(arg(0), "array"), position,
								cast(arg(1), dataType), currentVersion, staticMethods, compatibilityLevel)),
						call(arg(0), "position")
				)
		);

		defineDeserialize(serializerGen, asmFactory, allVersions, staticMethods);
		for (StaticMethods.Key key : staticMethods.mapDeserialize.keySet()) {
			StaticMethods.Value value = staticMethods.mapDeserialize.get(key);
			asmFactory.staticMethod(value.method,
					key.serializerGen.getRawType(),
					asList(SerializationInputBuffer.class),
					value.expression);
		}

		return asmFactory.newInstance();
	}

	private void defineDeserialize(final SerializerGen serializerGen, final AsmBuilder asmFactory, final List<Integer> allVersions, StaticMethods staticMethods) {
		defineDeserializeLatest(serializerGen, asmFactory, getLatestVersion(allVersions), staticMethods);

		defineDeserializeEarlierVersion(serializerGen, asmFactory, allVersions, staticMethods);
		for (int i = allVersions.size() - 2; i >= 0; i--) {
			int version = allVersions.get(i);
			defineDeserializeVersion(serializerGen, asmFactory, version, staticMethods);
		}
	}

	private void defineDeserializeVersion(SerializerGen serializerGen, AsmBuilder asmFactory, int version, StaticMethods staticMethods) {
		asmFactory.method("deserializeVersion" + String.valueOf(version),
				serializerGen.getRawType(),
				asList(SerializationInputBuffer.class),
				sequence(serializerGen.deserialize(serializerGen.getRawType(), version, staticMethods, compatibilityLevel)));
	}

	private void defineDeserializeEarlierVersion(SerializerGen serializerGen, AsmBuilder asmFactory, List<Integer> allVersions, StaticMethods staticMethods) {
		List<Expression> listKey = new ArrayList<>();
		List<Expression> listValue = new ArrayList<>();
		for (int i = allVersions.size() - 2; i >= 0; i--) {
			int version = allVersions.get(i);
			listKey.add(value(version));
			serializerGen.prepareDeserializeStaticMethods(version, staticMethods, compatibilityLevel);
			listValue.add(call(self(), "deserializeVersion" + String.valueOf(version), arg(0)));
		}
		asmFactory.method("deserializeEarlierVersions", serializerGen.getRawType(), asList(SerializationInputBuffer.class, int.class),
				switchForKey(arg(1), listKey, listValue));
	}

	private void defineDeserializeLatest(final SerializerGen serializerGen, final AsmBuilder asmFactory, final Integer latestVersion, StaticMethods staticMethods) {
		if (latestVersion == null) {
			serializerGen.prepareDeserializeStaticMethods(0, staticMethods, compatibilityLevel);
			asmFactory.method("deserialize", serializerGen.deserialize(serializerGen.getRawType(), 0, staticMethods, compatibilityLevel));
		} else {
			serializerGen.prepareDeserializeStaticMethods(latestVersion, staticMethods, compatibilityLevel);
			Expression version = let(call(arg(0), "readVarInt"));
			asmFactory.method("deserialize", sequence(version, choice(cmpEq(version, value(latestVersion)),
					serializerGen.deserialize(serializerGen.getRawType(), latestVersion, staticMethods, compatibilityLevel),
					call(self(), "deserializeEarlierVersions", arg(0), version))));
		}
	}

	private Integer getLatestVersion(List<Integer> versions) {
		return versions.isEmpty() ? null : versions.get(versions.size() - 1);
	}
}
