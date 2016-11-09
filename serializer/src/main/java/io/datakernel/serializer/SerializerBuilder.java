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
import io.datakernel.bytebuf.ByteBuf;
import io.datakernel.codegen.ClassBuilder;
import io.datakernel.codegen.DefiningClassLoader;
import io.datakernel.codegen.Expression;
import io.datakernel.codegen.Variable;
import io.datakernel.codegen.utils.Preconditions;
import io.datakernel.serializer.annotations.*;
import io.datakernel.serializer.asm.*;

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
import static io.datakernel.codegen.utils.Preconditions.check;
import static io.datakernel.codegen.utils.Preconditions.checkNotNull;
import static java.lang.Character.toUpperCase;
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

	public interface Helper {
		SerializerGen createSubclassesSerializer(Class<?> type, SerializeSubclasses serializeSubclasses);
	}

	private final Helper helper = new Helper() {
		@Override
		public SerializerGen createSubclassesSerializer(Class<?> type, SerializeSubclasses serializeSubclasses) {
			return SerializerBuilder.this.createSubclassesSerializer(type, serializeSubclasses);
		}
	};

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
		final SerializerBuilder builder = new SerializerBuilder(definingClassLoader);

		builder.setSerializer(Object.class, new SerializerGenBuilder() {
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
				builder.initTasks.add(new Runnable() {
					@Override
					public void run() {
						builder.scanAnnotations(type, generics, serializer);
					}
				});
				return serializer;
			}
		});
		builder.setSerializer(List.class, new SerializerGenBuilder() {
			@Override
			public SerializerGen serializer(Class<?> type, final SerializerForType[] generics, SerializerGen fallback) {
				check(generics.length == 1);
				return new SerializerGenList(generics[0].serializer);
			}
		});
		builder.setSerializer(Collection.class, new SerializerGenBuilder() {
			@Override
			public SerializerGen serializer(Class<?> type, final SerializerForType[] generics, SerializerGen fallback) {
				check(generics.length == 1);
				return new SerializerGenList(generics[0].serializer);
			}
		});
		builder.setSerializer(Set.class, new SerializerGenBuilder() {
			@Override
			public SerializerGen serializer(Class<?> type, SerializerForType[] generics, SerializerGen fallback) {
				check(generics.length == 1);
				return new SerializerGenSet(generics[0].serializer);
			}
		});
		builder.setSerializer(Map.class, new SerializerGenBuilder() {
			@Override
			public SerializerGen serializer(Class<?> type, final SerializerForType[] generics, SerializerGen fallback) {
				check(generics.length == 2);
				return new SerializerGenMap(generics[0].serializer, generics[1].serializer);
			}
		});
		builder.setSerializer(Enum.class, new SerializerGenBuilder() {
			@Override
			public SerializerGen serializer(final Class<?> type, final SerializerForType[] generics, SerializerGen fallback) {
				List<FoundSerializer> foundSerializers = builder.scanSerializers(type, generics);
				if (!foundSerializers.isEmpty()) {
					final SerializerGenClass serializer = new SerializerGenClass(type);
					builder.initTasks.add(new Runnable() {
						@Override
						public void run() {
							builder.scanAnnotations(type, generics, serializer);
						}
					});
					return serializer;
				} else {
					return new SerializerGenEnum(type);
				}
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
		builder.setAnnotationHandler(SerializeMaxLength.class, SerializeMaxLengthEx.class, new SerializeMaxLengthHandler());
		builder.setAnnotationHandler(SerializeStringFormat.class, SerializeStringFormatEx.class, new SerializeStringFormatHandler());
		return builder;
	}

	private <A extends Annotation, P extends Annotation> SerializerBuilder setAnnotationHandler(Class<A> annotation,
	                                                                                            Class<P> annotationPlural,
	                                                                                            AnnotationHandler<A, P> annotationHandler) {
		annotationsMap.put(annotation, annotationHandler);
		if (annotationPlural != null)
			annotationsExMap.put(annotation, annotationPlural);
		return this;
	}

	public SerializerBuilder withCompatibilityLevel(CompatibilityLevel compatibilityLevel) {
		this.compatibilityLevel = compatibilityLevel;
		return this;
	}

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

	private void setSerializer(Class<?> type, final SerializerGen serializer) {
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
		LinkedHashSet<Class<?>> subclassesSet = new LinkedHashSet<>();
		subclassesSet.addAll(subclasses);
		check(subclassesSet.size() == subclasses.size());
		SerializerGen subclassesSerializer = createSubclassesSerializer(type, subclassesSet, 0);
		setSerializer(type, subclassesSerializer);
	}

	public <T> void setSubclasses(Class<T> type, Class<? extends T>... subclasses) {
		setSubclasses(type, Arrays.asList(subclasses));
	}

	public <T> SerializerBuilder withSubclasses(Class<T> type, List<Class<? extends T>> subclasses) {
		setSubclasses(type, subclasses);
		return this;
	}

	public <T> SerializerBuilder withSubclasses(Class<T> type, Class<? extends T>... subclasses) {
		setSubclasses(type, subclasses);
		return this;
	}

	public SerializerBuilder withHppcSupport() {
		registerHppcMaps();
		registerHppcSets();
		return this;
	}

	private void registerHppcMaps() {
		List<Class<?>> types = asList(
				byte.class, short.class, int.class, long.class, float.class, double.class, char.class, Object.class
		);

		for (int i = 0; i < types.size(); i++) {
			Class<?> keyType = types.get(i);
			String keyTypeName = keyType.getSimpleName();
			for (Class<?> valueType : types) {
				String valueTypeName = valueType.getSimpleName();
				String hppcMapTypeName
						= "com.carrotsearch.hppc." + capitalize(keyTypeName) + capitalize(valueTypeName) + "Map";
				Class<?> hppcMapType;
				try {
					hppcMapType = Class.forName(hppcMapTypeName, true, definingClassLoader);
				} catch (ClassNotFoundException e) {
					throw new IllegalStateException("Cannot load " + e.getClass().getName(), e);
				}
				typeMap.put(hppcMapType, SerializerGenHppcMap.serializerGenBuilder(hppcMapType, keyType, valueType));
			}
		}
	}

	private void registerHppcSets() {
		List<Class<?>> types = asList(
				byte.class, short.class, int.class, long.class, float.class, double.class, char.class, Object.class
		);

		for (Class<?> valueType : types) {
			String valueTypeName = valueType.getSimpleName();
			String hppcSetTypeName = "com.carrotsearch.hppc." + capitalize(valueTypeName) + "Set";
			Class<?> hppcSetType;
			try {
				hppcSetType = Class.forName(hppcSetTypeName, true, definingClassLoader);
			} catch (ClassNotFoundException e) {
				throw new IllegalStateException(e);
			}
			typeMap.put(hppcSetType, SerializerGenHppcSet.serializerGenBuilder(hppcSetType, valueType));
		}
	}

	private static String capitalize(String str) {
		return String.valueOf(toUpperCase(str.charAt(0))) + str.substring(1);
	}

	/**
	 * Creates a {@code SerializerGen} for the given type token.
	 *
	 * @return {@code SerializerGen} for the given type token
	 */
	public <T> BufferSerializer<T> build(Class<T> type) {
		SerializerGenBuilder.SerializerForType[] serializerForTypes = new SerializerGenBuilder.SerializerForType[0];
		return build(type, serializerForTypes);
	}

	public <T> BufferSerializer<T> build(SerializerGen serializerGen) {
		return buildBufferSerializer(serializerGen, version);
	}

	public <T> BufferSerializer<T> build(Class<?> type, SerializerGenBuilder.SerializerForType[] generics) {
		return buildBufferSerializer(createSerializerGen(type, generics, Collections.<SerializerGenBuilder>emptyList()), version);
	}

	private SerializerGen createSerializerGen(Class<?> type, SerializerGenBuilder.SerializerForType[] generics, List<SerializerGenBuilder> mods) {
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

	private SerializerGen createNewSerializer(Class<?> type, SerializerGenBuilder.SerializerForType[] generics, List<SerializerGenBuilder> mods) {
		if (!mods.isEmpty()) {
			SerializerGen serializer = createSerializerGen(type, generics, mods.subList(0, mods.size() - 1));
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

	private SerializerGen createSubclassesSerializer(Class<?> type, SerializeSubclasses serializeSubclasses) {
		LinkedHashSet<Class<?>> subclassesSet = new LinkedHashSet<>();
		subclassesSet.addAll(Arrays.asList(serializeSubclasses.value()));
		check(subclassesSet.size() == serializeSubclasses.value().length);

		if (!serializeSubclasses.extraSubclassesId().isEmpty()) {
			Collection<Class<?>> registeredSubclasses = extraSubclassesMap.get(serializeSubclasses.extraSubclassesId());
			if (registeredSubclasses != null)
				subclassesSet.addAll(registeredSubclasses);
		}
		return createSubclassesSerializer(type, subclassesSet, serializeSubclasses.startIndex());
	}

	private SerializerGen createSubclassesSerializer(Class<?> type, LinkedHashSet<Class<?>> subclassesSet,
	                                                 int startIndex) {
		checkNotNull(subclassesSet);
		check(!subclassesSet.isEmpty());
		LinkedHashMap<Class<?>, SerializerGen> subclasses = new LinkedHashMap<>();
		for (Class<?> subclass : subclassesSet) {
			check(subclass.getTypeParameters().length == 0);
			check(type.isAssignableFrom(subclass), "Unrelated subclass '%s' for '%s'", subclass, type);

			SerializerGen serializer = createSerializerGen(
					subclass,
					new SerializerGenBuilder.SerializerForType[]{},
					Collections.<SerializerGenBuilder>emptyList()
			);
			subclasses.put(subclass, serializer);
		}
		return new SerializerGenSubclass(type, subclasses, startIndex);
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
					SerializerGenBuilder serializerGenBuilder = annotationHandler.createBuilder(helper, annotation, compatibilityLevel);
					TypedModsMap.Builder child = rootBuilder.ensureChild(annotationHandler.extractPath(annotation));
					child.add(serializerGenBuilder);
				}
			}
			for (Annotation annotationEx : annotations) {
				if (annotationEx.annotationType() == annotationExType) {
					for (Annotation annotation : annotationHandler.extractList(annotationEx)) {
						SerializerGenBuilder serializerGenBuilder = annotationHandler.createBuilder(helper, annotation, compatibilityLevel);
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
			SerializerGen serializer = createSerializerGen(rawType, typeArguments, typedModsMap.getMods());
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
			SerializerGen serializer = createSerializerGen(rawType, generics, typedModsMap.getMods());
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

	private <T> BufferSerializer<T> buildBufferSerializer(SerializerGen serializerGen, int serializeVersion) {
		return (BufferSerializer<T>) createSerializer(serializerGen, serializeVersion);
	}

	/**
	 * Constructs buffer serializer for type, described by the given {@code SerializerGen}.
	 *
	 * @param serializerGen {@code SerializerGen} that describes the type that is to serialize
	 * @return buffer serializer for the given {@code SerializerGen}
	 */
	private <T> BufferSerializer<T> buildBufferSerializer(SerializerGen serializerGen) {
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
		ClassBuilder<BufferSerializer> asmFactory = ClassBuilder.create(definingClassLoader, BufferSerializer.class);
		if (saveBytecodePath != null) {
			asmFactory = asmFactory.withBytecodeSaveDir(saveBytecodePath);
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
			version = call(arg(0), "writeVarInt", value(currentVersion));
		}

		StaticMethods staticMethods = new StaticMethods();

		if (currentVersion == null) currentVersion = 0;
		serializerGen.prepareSerializeStaticMethods(currentVersion, staticMethods, compatibilityLevel);
		for (StaticMethods.Key key : staticMethods.mapSerialize.keySet()) {
			StaticMethods.Value value = staticMethods.mapSerialize.get(key);
			asmFactory = asmFactory.withStaticMethod(value.method,
					int.class,
					asList(byte[].class, int.class, key.serializerGen.getRawType()),
					value.expression);
		}
		Variable position = let(call(arg(0), "writePosition"));
		asmFactory = asmFactory.withMethod("serialize", sequence(version,
						call(arg(0), "writePosition", serializerGen.serialize(
								call(arg(0), "array"), position,
								cast(arg(1), dataType), currentVersion, staticMethods, compatibilityLevel)),
						call(arg(0), "writePosition")
				)
		);

		asmFactory = defineDeserialize(serializerGen, asmFactory, allVersions, staticMethods);
		for (StaticMethods.Key key : staticMethods.mapDeserialize.keySet()) {
			StaticMethods.Value value = staticMethods.mapDeserialize.get(key);
			asmFactory = asmFactory.withStaticMethod(value.method,
					key.serializerGen.getRawType(),
					asList(ByteBuf.class),
					value.expression);
		}

		return asmFactory.buildClassAndCreateNewInstance();
	}

	private ClassBuilder<BufferSerializer> defineDeserialize(final SerializerGen serializerGen,
	                                                         ClassBuilder<BufferSerializer> asmFactory,
	                                                         final List<Integer> allVersions,
	                                                         StaticMethods staticMethods) {
		asmFactory = defineDeserializeLatest(serializerGen, asmFactory, getLatestVersion(allVersions), staticMethods);

		asmFactory = defineDeserializeEarlierVersion(serializerGen, asmFactory, allVersions, staticMethods);
		for (int i = allVersions.size() - 2; i >= 0; i--) {
			int version = allVersions.get(i);
			asmFactory = defineDeserializeVersion(serializerGen, asmFactory, version, staticMethods);
		}
		return asmFactory;
	}

	private ClassBuilder<BufferSerializer> defineDeserializeVersion(SerializerGen serializerGen,
	                                                                ClassBuilder<BufferSerializer> asmFactory,
	                                                                int version, StaticMethods staticMethods) {
		return asmFactory.withMethod("deserializeVersion" + String.valueOf(version),
				serializerGen.getRawType(),
				asList(ByteBuf.class),
				sequence(serializerGen.deserialize(serializerGen.getRawType(), version, staticMethods, compatibilityLevel)));
	}

	private ClassBuilder<BufferSerializer> defineDeserializeEarlierVersion(SerializerGen serializerGen,
	                                                                       ClassBuilder<BufferSerializer> asmFactory,
	                                                                       List<Integer> allVersions,
	                                                                       StaticMethods staticMethods) {
		List<Expression> listKey = new ArrayList<>();
		List<Expression> listValue = new ArrayList<>();
		for (int i = allVersions.size() - 2; i >= 0; i--) {
			int version = allVersions.get(i);
			listKey.add(value(version));
			serializerGen.prepareDeserializeStaticMethods(version, staticMethods, compatibilityLevel);
			listValue.add(call(self(), "deserializeVersion" + String.valueOf(version), arg(0)));
		}
		return asmFactory.withMethod("deserializeEarlierVersions", serializerGen.getRawType(), asList(ByteBuf.class, int.class),
				switchForKey(arg(1), listKey, listValue));
	}

	private ClassBuilder<BufferSerializer> defineDeserializeLatest(final SerializerGen serializerGen,
	                                                               final ClassBuilder<BufferSerializer> asmFactory,
	                                                               final Integer latestVersion,
	                                                               StaticMethods staticMethods) {
		if (latestVersion == null) {
			serializerGen.prepareDeserializeStaticMethods(0, staticMethods, compatibilityLevel);
			return asmFactory
					.withMethod("deserialize", serializerGen.deserialize(
							serializerGen.getRawType(), 0, staticMethods, compatibilityLevel));
		} else {
			serializerGen.prepareDeserializeStaticMethods(latestVersion, staticMethods, compatibilityLevel);
			Expression version = let(call(arg(0), "readVarInt"));
			return asmFactory.withMethod("deserialize", sequence(version, ifThenElse(cmpEq(version, value(latestVersion)),
					serializerGen.deserialize(serializerGen.getRawType(), latestVersion, staticMethods, compatibilityLevel),
					call(self(), "deserializeEarlierVersions", arg(0), version))));
		}
	}

	private Integer getLatestVersion(List<Integer> versions) {
		return versions.isEmpty() ? null : versions.get(versions.size() - 1);
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

}
