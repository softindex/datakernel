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

import io.datakernel.codegen.ClassBuilder;
import io.datakernel.codegen.DefiningClassLoader;
import io.datakernel.codegen.Expression;
import io.datakernel.codegen.Variable;
import io.datakernel.serializer.SerializerDef.StaticDecoders;
import io.datakernel.serializer.TypedModsMap.Builder;
import io.datakernel.serializer.annotations.*;
import io.datakernel.serializer.impl.*;
import io.datakernel.serializer.impl.SerializerDefBuilder.SerializerForType;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.lang.annotation.Annotation;
import java.lang.reflect.*;
import java.net.Inet4Address;
import java.net.Inet6Address;
import java.net.InetAddress;
import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.util.*;

import static io.datakernel.codegen.Expressions.*;
import static io.datakernel.common.Preconditions.checkArgument;
import static io.datakernel.common.Preconditions.checkNotNull;
import static io.datakernel.common.Utils.nullToDefault;
import static io.datakernel.common.Utils.of;
import static io.datakernel.serializer.Utils.findAnnotation;
import static io.datakernel.serializer.impl.SerializerExpressions.readByte;
import static io.datakernel.serializer.impl.SerializerExpressions.writeByte;
import static java.lang.reflect.Modifier.*;
import static java.util.Arrays.asList;

/**
 * Scans fields of classes for serialization.
 */
@SuppressWarnings("ArraysAsListWithZeroOrOneArgument")
public final class SerializerBuilder {
	private final DefiningClassLoader classLoader;
	private String profile;
	private int version = Integer.MAX_VALUE;
	private Path saveBytecodePath;
	private CompatibilityLevel compatibilityLevel = CompatibilityLevel.LEVEL_3;
	private Object[] classKey = null;

	private final Map<Class<?>, SerializerDefBuilder> typeMap = new LinkedHashMap<>();
	private final Map<Class<? extends Annotation>, Class<? extends Annotation>> annotationsExMap = new LinkedHashMap<>();
	private final Map<Class<? extends Annotation>, AnnotationHandler<?, ?>> annotationsMap = new LinkedHashMap<>();
	private final Map<String, Collection<Class<?>>> extraSubclassesMap = new HashMap<>();

	private final Map<Key, SerializerDef> cachedSerializers = new HashMap<>();
	private final List<Runnable> initTasks = new ArrayList<>();

	@FunctionalInterface
	public interface Helper {
		SerializerDef createSubclassesSerializer(Class<?> type, SerializeSubclasses serializeSubclasses);
	}

	private final Helper helper = this::createSubclassesSerializer;

	private SerializerBuilder(DefiningClassLoader classLoader) {
		this.classLoader = classLoader;
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

		builder.setSerializer(Object.class, (type, generics, target) -> {
			checkArgument(type.getTypeParameters().length == generics.length, "Number of type parameters should be equal to number of generics");
			checkArgument(target == null, "Target must be null");
			SerializeInterface annotation = findAnnotation(SerializeInterface.class, type.getAnnotations());
			SerializerDefClass serializer = annotation != null && annotation.impl() != void.class ?
					SerializerDefClass.of(type, annotation.impl()) :
					SerializerDefClass.of(type);
			builder.initTasks.add(() -> builder.scanAnnotations(type, generics, serializer));
			return serializer;
		});
		builder.setSerializer(List.class, (type, generics, target) -> {
			checkArgument(generics.length == 1, "List must have 1 generic type parameter");
			return new SerializerDefList(generics[0].serializer);
		});
		builder.setSerializer(Collection.class, (type, generics, target) -> {
			checkArgument(generics.length == 1, "Collection must have 1 generic type parameter");
			return new SerializerDefList(generics[0].serializer);
		});
		builder.setSerializer(Set.class, (type, generics, target) -> {
			checkArgument(generics.length == 1, "Set must have 1 generic type parameter");
			return new SerializerDefSet(generics[0].serializer);
		});
		builder.setSerializer(Map.class, (type, generics, target) -> {
			checkArgument(generics.length == 2, "Map must have 2 generic type parameter");
			return new SerializerDefMap(generics[0].serializer, generics[1].serializer);
		});
		builder.setSerializer(Enum.class, (type, generics, target) -> {
			List<FoundSerializer> foundSerializers = builder.scanSerializers(type, generics);
			if (!foundSerializers.isEmpty()) {
				SerializerDefClass serializer = SerializerDefClass.of(type);
				builder.initTasks.add(() -> builder.scanAnnotations(type, generics, serializer));
				return serializer;
			} else {
				return new SerializerDefEnum(type);
			}
		});
		builder.setSerializer(boolean.class, new SerializerDefBoolean(false));
		builder.setSerializer(char.class, new SerializerDefChar(false));
		builder.setSerializer(byte.class, new SerializerDefByte(false));
		builder.setSerializer(short.class, new SerializerDefShort(false));
		builder.setSerializer(int.class, new SerializerDefInt(false, false));
		builder.setSerializer(long.class, new SerializerDefLong(false, false));
		builder.setSerializer(float.class, new SerializerDefFloat(false));
		builder.setSerializer(double.class, new SerializerDefDouble(false));
		builder.setSerializer(Boolean.class, new SerializerDefBoolean(true));
		builder.setSerializer(Character.class, new SerializerDefChar(true));
		builder.setSerializer(Byte.class, new SerializerDefByte(true));
		builder.setSerializer(Short.class, new SerializerDefShort(true));
		builder.setSerializer(Integer.class, new SerializerDefInt(true, false));
		builder.setSerializer(Long.class, new SerializerDefLong(true, false));
		builder.setSerializer(Float.class, new SerializerDefFloat(true));
		builder.setSerializer(Double.class, new SerializerDefDouble(true));
		builder.setSerializer(String.class, new SerializerDefString());
		builder.setSerializer(Inet4Address.class, new SerializerDefInet4Address());
		builder.setSerializer(Inet6Address.class, new SerializerDefInet6Address());

		LinkedHashMap<Class<?>, SerializerDef> addressMap = new LinkedHashMap<>();
		addressMap.put(Inet4Address.class, new SerializerDefInet4Address());
		addressMap.put(Inet6Address.class, new SerializerDefInet6Address());
		builder.setSerializer(InetAddress.class, new SerializerDefSubclass(InetAddress.class, addressMap, 0));

		builder.setSerializer(ByteBuffer.class, new SerializerDefByteBuffer());

		builder.setAnnotationHandler(SerializerClass.class, SerializerClassEx.class, new SerializerClassHandler());
		builder.setAnnotationHandler(SerializeFixedSize.class, SerializeFixedSizeEx.class, new SerializeFixedSizeHandler());
		builder.setAnnotationHandler(SerializeVarLength.class, SerializeVarLengthEx.class, new SerializeVarLengthHandler());
		builder.setAnnotationHandler(SerializeSubclasses.class, SerializeSubclassesEx.class, new SerializeSubclassesHandler());
		builder.setAnnotationHandler(SerializeNullable.class, SerializeNullableEx.class, new SerializeNullableHandler());
		builder.setAnnotationHandler(SerializeStringFormat.class, SerializeStringFormatEx.class, new SerializeStringFormatHandler());
		return builder;
	}

	private <A extends Annotation, P extends Annotation> void setAnnotationHandler(Class<A> annotation,
			Class<P> annotationPlural,
			AnnotationHandler<A, P> annotationHandler) {
		annotationsMap.put(annotation, annotationHandler);
		if (annotationPlural != null) {
			annotationsExMap.put(annotation, annotationPlural);
		}
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
	public SerializerBuilder withGeneratedBytecodePath(Path path) {
		this.saveBytecodePath = path;
		return this;
	}

	public SerializerBuilder withVersion(int version) {
		this.version = version;
		return this;
	}

	public SerializerBuilder withDefaultStringFormat(StringFormat format) {
		setSerializer(String.class, new SerializerDefString(format));
		return this;
	}

	public SerializerBuilder withProfile(String profile) {
		this.profile = profile;
		return this;
	}

	public SerializerBuilder withClassKey(Object... classKeyParameters) {
		this.classKey = classKeyParameters;
		return this;
	}

	private void setSerializer(Class<?> type, SerializerDef serializer) {
		setSerializer(type, SerializerDefBuilder.of(serializer));
	}

	private void setSerializer(Class<?> type, SerializerDefBuilder serializer) {
		typeMap.put(type, serializer);
	}

	public SerializerBuilder withSerializer(Class<?> type, SerializerDefBuilder serializer) {
		typeMap.put(type, serializer);
		return this;
	}

	public SerializerBuilder withSerializer(Class<?> type, SerializerDef serializer) {
		return withSerializer(type, SerializerDefBuilder.of(serializer));
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
		checkArgument(subclassesSet.size() == subclasses.size(), "Subclasses should be unique");
		SerializerDef subclassesSerializer = createSubclassesSerializer(type, subclassesSet, 0);
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

	public <T> BinarySerializer<T> build(Class<?> type, SerializerForType[] generics) {
		SerializerDef serializer = createSerializerDef(type, generics, Collections.emptyList());
		//noinspection unchecked
		return (BinarySerializer<T>) buildImpl(serializer, version);
	}

	public <T> BinarySerializer<T> build(SerializerDef serializer) {
		//noinspection unchecked
		return (BinarySerializer<T>) buildImpl(serializer, version);
	}

	private SerializerDef createSerializerDef(Class<?> type, SerializerForType[] generics, List<SerializerDefBuilder> mods) {
		Key key = new Key(type, generics, mods);
		SerializerDef serializer = cachedSerializers.get(key);
		if (serializer == null) {
			serializer = createNewSerializer(type, generics, mods);
			cachedSerializers.put(key, serializer);
		}
		while (!initTasks.isEmpty()) {
			initTasks.remove(0).run();
		}
		return serializer;
	}

	private SerializerDef createNewSerializer(Class<?> type, SerializerForType[] generics, List<SerializerDefBuilder> mods) {
		if (!mods.isEmpty()) {
			SerializerDef serializer = createSerializerDef(type, generics, mods.subList(0, mods.size() - 1));
			SerializerDefBuilder last = mods.get(mods.size() - 1);
			return last.serializer(type, generics, serializer);
		}

		if (type.isArray()) {
			checkArgument(generics.length == 1, "Number of generics should be equal to 1");
			SerializerDef itemSerializer = generics[0].serializer;
			return new SerializerDefArray(itemSerializer, type);
		}

		SerializeSubclasses serializeSubclasses = findAnnotation(SerializeSubclasses.class, type.getAnnotations());
		if (serializeSubclasses != null) {
			return createSubclassesSerializer(type, serializeSubclasses);
		}

		Class<?> key = findKey(type, typeMap.keySet());
		SerializerDefBuilder builder = typeMap.get(key);
		if (builder == null) {
			throw new IllegalArgumentException("No builder for type " + key);
		}
		SerializerDef serializer = builder.serializer(type, generics, null);
		return checkNotNull(serializer);
	}

	private SerializerDef createSubclassesSerializer(Class<?> type, SerializeSubclasses serializeSubclasses) {
		LinkedHashSet<Class<?>> subclassesSet = new LinkedHashSet<>(Arrays.asList(serializeSubclasses.value()));
		checkArgument(subclassesSet.size() == serializeSubclasses.value().length, "Subclasses should be unique");

		if (!serializeSubclasses.extraSubclassesId().isEmpty()) {
			Collection<Class<?>> registeredSubclasses = extraSubclassesMap.get(serializeSubclasses.extraSubclassesId());
			if (registeredSubclasses != null) {
				subclassesSet.addAll(registeredSubclasses);
			}
		}
		return createSubclassesSerializer(type, subclassesSet, serializeSubclasses.startIndex());
	}

	private SerializerDef createSubclassesSerializer(Class<?> type, @NotNull LinkedHashSet<Class<?>> subclassesSet,
			int startIndex) {
		checkArgument(!subclassesSet.isEmpty(), "Set of subclasses should not be empty");
		LinkedHashMap<Class<?>, SerializerDef> subclasses = new LinkedHashMap<>();
		for (Class<?> subclass : subclassesSet) {
			checkArgument(subclass.getTypeParameters().length == 0, "Subclass should have no type parameters");
			checkArgument(type.isAssignableFrom(subclass), "Unrelated subclass '%s' for '%s'", subclass, type);

			subclasses.put(subclass, createSerializerDef(
					subclass,
					new SerializerForType[]{},
					Collections.emptyList()));
		}
		return new SerializerDefSubclass(type, subclasses, startIndex);
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
					SerializerDefBuilder serializerDefBuilder = annotationHandler.createBuilder(helper, annotation, compatibilityLevel);
					Builder child = rootBuilder.ensureChild(annotationHandler.extractPath(annotation));
					child.add(serializerDefBuilder);
				}
			}
			for (Annotation annotationEx : annotations) {
				if (annotationEx.annotationType() == annotationExType) {
					for (Annotation annotation : annotationHandler.extractList(annotationEx)) {
						SerializerDefBuilder serializerDefBuilder = annotationHandler.createBuilder(helper, annotation, compatibilityLevel);
						Builder child = rootBuilder.ensureChild(annotationHandler.extractPath(annotation));
						child.add(serializerDefBuilder);
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
			checkArgument(i < classType.getTypeParameters().length, "No type variable '%s' is found in type parameters of %s", typeVariableName, classType);

			SerializerDef serializer = typedModsMap.rewrite(classGenerics[i].rawType, new SerializerForType[]{}, classGenerics[i].serializer);
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
			SerializerDef serializer = createSerializerDef(rawType, typeArguments, typedModsMap.getMods());
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
			SerializerDef serializer = createSerializerDef(rawType, generics, typedModsMap.getMods());
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
		SerializerDef serializer;

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

		Serialize serialize = findAnnotation(Serialize.class, annotations);
		if (serialize != null) {
			added = serialize.added();
			removed = serialize.removed();
		}

		SerializeProfiles profiles = findAnnotation(SerializeProfiles.class, annotations);
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
		checkArgument(isPublic(field.getModifiers()), "Field %s must be public", field);
		checkArgument(!isStatic(field.getModifiers()), "Field %s must not be static", field);
		checkArgument(!isTransient(field.getModifiers()), "Field %s must not be transient", field);
		result.serializer = resolveSerializer(classType, classGenerics, field.getGenericType(), result.mods).serializer;
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
		checkArgument(isPublic(getter.getModifiers()), "Getter %s must be public", getter);
		checkArgument(!isStatic(getter.getModifiers()), "Getter %s must not be static", getter);
		checkArgument(getter.getReturnType() != Void.TYPE && getter.getParameterTypes().length == 0, "%s must be getter", getter);

		result.serializer = resolveSerializer(classType, classGenerics, getter.getGenericReturnType(), result.mods).serializer;

		return result;
	}

	private void scanAnnotations(Class<?> classType, SerializerForType[] classGenerics, SerializerDefClass serializer) {
		if (classType.isInterface()) {
			SerializeInterface annotation = findAnnotation(SerializeInterface.class, classType.getAnnotations());
			scanInterface(classType, classGenerics, serializer, (annotation != null) && annotation.inherit());
			if (annotation != null) {
				Class<?> impl = annotation.impl();
				if (impl == void.class) {
					return;
				}
				scanSetters(impl, serializer);
				scanFactories(impl, serializer);
				scanConstructors(impl, serializer);
				serializer.addMatchingSetters();
			}
			return;
		}
		checkArgument(!classType.isAnonymousClass(), "Class should not be anonymous");
		checkArgument(!classType.isLocalClass(), "Class should not be local");
		scanClass(classType, classGenerics, serializer);
		scanFactories(classType, serializer);
		scanConstructors(classType, serializer);
		serializer.addMatchingSetters();
	}

	private void scanInterface(Class<?> classType, SerializerForType[] classGenerics, SerializerDefClass serializer, boolean inheritSerializers) {
		List<FoundSerializer> foundSerializers = new ArrayList<>();
		scanGetters(classType, classGenerics, foundSerializers);
		addMethodsAndGettersToClass(serializer, foundSerializers);
		if (!inheritSerializers) {
			return;
		}

		SerializeInterface annotation = findAnnotation(SerializeInterface.class, classType.getAnnotations());
		if (annotation != null && !annotation.inherit()) {
			return;
		}
		for (Class<?> inter : classType.getInterfaces()) {
			scanInterface(inter, classGenerics, serializer, true);
		}
	}

	private void addMethodsAndGettersToClass(SerializerDefClass serializer, List<FoundSerializer> foundSerializers) {
		Set<Integer> orders = new HashSet<>();
		for (FoundSerializer foundSerializer : foundSerializers) {
			checkArgument(foundSerializer.order >= 0, "Invalid order %s for %s in %s", foundSerializer.order, foundSerializer, serializer);
			checkArgument(orders.add(foundSerializer.order), "Duplicate order %s for %s in %s", foundSerializer.order, foundSerializer, serializer);
		}
		Collections.sort(foundSerializers);
		for (FoundSerializer foundSerializer : foundSerializers) {
			if (foundSerializer.methodOrField instanceof Method) {
				serializer.addGetter((Method) foundSerializer.methodOrField, foundSerializer.serializer, foundSerializer.added, foundSerializer.removed);
			} else if (foundSerializer.methodOrField instanceof Field) {
				serializer.addField((Field) foundSerializer.methodOrField, foundSerializer.serializer, foundSerializer.added, foundSerializer.removed);
			} else {
				throw new AssertionError();
			}
		}
	}

	private void scanClass(Class<?> classType, SerializerForType[] classGenerics, SerializerDefClass serializer) {
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
			scanClass(classType.getSuperclass(), superclassGenerics, serializer);
		} else if (genericSuperclass instanceof Class) {
			scanClass(classType.getSuperclass(), new SerializerForType[]{}, serializer);
		} else {
			throw new IllegalArgumentException("Unsupported type " + genericSuperclass);
		}

		List<FoundSerializer> foundSerializers = scanSerializers(classType, classGenerics);
		addMethodsAndGettersToClass(serializer, foundSerializers);
		scanSetters(classType, serializer);
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
		for (Method method : classType.getDeclaredMethods()) {
			FoundSerializer foundSerializer = tryAddGetter(classType, classGenerics, method);
			if (foundSerializer != null) {
				foundSerializers.add(foundSerializer);
			}
		}
	}

	private void scanSetters(Class<?> classType, SerializerDefClass serializer) {
		for (Method method : classType.getDeclaredMethods()) {
			if (isStatic(method.getModifiers())) {
				continue;
			}
			if (method.getParameterTypes().length != 0) {
				List<String> fields = new ArrayList<>(method.getParameterTypes().length);
				for (int i = 0; i < method.getParameterTypes().length; i++) {
					Annotation[] parameterAnnotations = method.getParameterAnnotations()[i];
					Deserialize annotation = findAnnotation(Deserialize.class, parameterAnnotations);
					if (annotation != null) {
						String field = annotation.value();
						fields.add(field);
					}
				}
				if (fields.size() == method.getParameterTypes().length) {
					serializer.addSetter(method, fields);
				} else {
					checkArgument(fields.isEmpty(), "Fields should not be empty");
				}
			}
		}
	}

	private void scanFactories(Class<?> classType, SerializerDefClass serializer) {
		DeserializeFactory annotationFactory = findAnnotation(DeserializeFactory.class, classType.getAnnotations());
		Class<?> factoryClassType = (annotationFactory == null) ? classType : annotationFactory.value();
		for (Method factory : factoryClassType.getDeclaredMethods()) {
			if (classType != factory.getReturnType()) {
				continue;
			}
			if (factory.getParameterTypes().length != 0) {
				List<String> fields = new ArrayList<>(factory.getParameterTypes().length);
				for (int i = 0; i < factory.getParameterTypes().length; i++) {
					Annotation[] parameterAnnotations = factory.getParameterAnnotations()[i];
					Deserialize annotation = findAnnotation(Deserialize.class, parameterAnnotations);
					if (annotation != null) {
						String field = annotation.value();
						fields.add(field);
					}
				}
				if (fields.size() == factory.getParameterTypes().length) {
					serializer.setFactory(factory, fields);
				} else {
					checkArgument(fields.isEmpty(), "@Deserialize is not fully specified for %s", fields);
				}
			}
		}
	}

	private void scanConstructors(Class<?> classType, SerializerDefClass serializer) {
		boolean found = false;
		for (Constructor<?> constructor : classType.getDeclaredConstructors()) {
			List<String> fields = new ArrayList<>(constructor.getParameterTypes().length);
			for (int i = 0; i < constructor.getParameterTypes().length; i++) {
				Annotation[] parameterAnnotations = constructor.getParameterAnnotations()[i];
				Deserialize annotation = findAnnotation(Deserialize.class, parameterAnnotations);
				if (annotation != null) {
					String field = annotation.value();
					fields.add(field);
				}
			}
			if (constructor.getParameterTypes().length != 0 && fields.size() == constructor.getParameterTypes().length) {
				checkArgument(!found, "Duplicate @Deserialize constructor %s", constructor);
				found = true;
				serializer.setConstructor(constructor, fields);
			} else {
				checkArgument(fields.isEmpty(), "@Deserialize is not fully specified for %s", fields);
			}
		}
	}

	private BinarySerializer<?> buildImpl(SerializerDef serializer, int serializeVersion) {
		checkArgument(serializeVersion >= 0, "serializerVersion is negative");

		ClassBuilder<BinarySerializer> classBuilder = ClassBuilder.create(classLoader, BinarySerializer.class).withClassKey(classKey);
		if (saveBytecodePath != null) {
			classBuilder.withBytecodeSaveDir(saveBytecodePath);
		}

		Set<Integer> collectedVersions = new HashSet<>();
		SerializerDef.Visitor visitor = new SerializerDef.Visitor() {
			@Override
			public void visit(String serializerId, SerializerDef serializer) {
				collectedVersions.addAll(serializer.getVersions());
				serializer.accept(this);
			}
		};
		visitor.visit(serializer);

		List<Integer> versions = new ArrayList<>(collectedVersions);
		List<Integer> allVersions = new ArrayList<>(collectedVersions);
		versions.removeIf(v -> v > serializeVersion);
		Collections.sort(versions);
		Collections.sort(allVersions);
		Integer currentVersion = !allVersions.isEmpty() && versions.isEmpty() ?
				Integer.valueOf(serializeVersion) :
				getLatestVersion(versions);

		defineEncoders(classBuilder, serializer, currentVersion);

		defineDecoders(classBuilder, serializer, allVersions);

		return classBuilder.buildClassAndCreateNewInstance();
	}

	private void defineEncoders(ClassBuilder<?> classBuilder, SerializerDef serializer, @Nullable Integer currentVersion) {
		classBuilder.withMethod("encode", int.class, asList(byte[].class, int.class, Object.class),
				let(cast(arg(2), serializer.getEncodeType()), data ->
						encoderImpl(classBuilder, serializer, currentVersion, arg(0), arg(1), data)));

		classBuilder.withMethod("encode", void.class, asList(BinaryOutput.class, Object.class),
				let(call(arg(0), "array"), buf ->
						let(call(arg(0), "pos"), pos ->
								let(cast(arg(1), serializer.getEncodeType()), data ->
										sequence(
												encoderImpl(classBuilder, serializer, currentVersion, buf, pos, data),
												call(arg(0), "pos", pos))))));
	}

	private Expression encoderImpl(ClassBuilder<?> classBuilder, SerializerDef serializer, @Nullable Integer currentVersion, Expression buf, Variable pos, Expression data) {
		return sequence(
				currentVersion != null ?
						writeByte(buf, pos, value((byte) (int) currentVersion)) :
						sequence(),

				serializer.encoder(
						staticEncoders(classBuilder),
						buf,
						pos,
						data,
						nullToDefault(currentVersion, 0),
						compatibilityLevel),

				pos);
	}

	private void defineDecoders(ClassBuilder<?> classBuilder, SerializerDef serializer, List<Integer> allVersions) {
		Integer latestVersion = getLatestVersion(allVersions);
		classBuilder.withMethod("decode", Object.class, asList(BinaryInput.class),
				decodeImpl(classBuilder, serializer, latestVersion, arg(0)));

		classBuilder.withMethod("decode", Object.class, asList(byte[].class, int.class),
				let(constructor(BinaryInput.class, arg(0), arg(1)), in ->
						decodeImpl(classBuilder, serializer, latestVersion, in)));

		classBuilder.withMethod("decodeEarlierVersions",
				serializer.getDecodeType(),
				asList(BinaryInput.class, byte.class),
				of(() -> {
					List<Expression> listKey = new ArrayList<>();
					List<Expression> listValue = new ArrayList<>();
					for (int i = allVersions.size() - 2; i >= 0; i--) {
						int version = allVersions.get(i);
						listKey.add(value((byte) version));
						listValue.add(call(self(), "decodeVersion" + version, arg(0)));
					}
					return switchByKey(arg(1), listKey, listValue);
				}));

		for (int i = allVersions.size() - 2; i >= 0; i--) {
			int version = allVersions.get(i);
			classBuilder.withMethod("decodeVersion" + version, serializer.getDecodeType(), asList(BinaryInput.class),
					sequence(serializer.defineDecoder(staticDecoders(classBuilder, version),
							arg(0), version, compatibilityLevel)));
		}
	}

	private Expression decodeImpl(ClassBuilder<?> classBuilder, SerializerDef serializer, Integer latestVersion, Expression in) {
		return latestVersion == null ?
				serializer.decoder(
						staticDecoders(classBuilder, null),
						in,
						0,
						compatibilityLevel) :

				let(readByte(in),
						version -> ifThenElse(cmpEq(version, value((byte) (int) latestVersion)),
								serializer.decoder(
										staticDecoders(classBuilder, null),
										in,
										latestVersion,
										compatibilityLevel),
								call(self(), "decodeEarlierVersions", in, version)));
	}

	private static SerializerDef.StaticEncoders staticEncoders(ClassBuilder<?> classBuilder) {
		return new SerializerDef.StaticEncoders() {
			@Override
			public Expression define(Class<?> valueClazz, Expression buf, Variable pos, Expression value, Expression method) {
				String methodName;
				for (int i = 1; ; i++) {
					methodName = "encode_" +
							valueClazz.getSimpleName().replace('[', 's').replace(']', '_') +
							(i == 1 ? "" : "_" + i);
					String _methodName = methodName;
					if (classBuilder.getStaticMethods().keySet().stream().noneMatch(m -> m.getName().equals(_methodName)))
						break;
				}
				classBuilder.withStaticMethod(methodName, int.class, asList(byte[].class, int.class, valueClazz),
						sequence(method, POS));
				return set(pos, staticCallSelf(methodName, buf, pos, cast(value, valueClazz)));
			}
		};
	}

	private StaticDecoders staticDecoders(ClassBuilder<?> classBuilder, @Nullable Integer version) {
		return new StaticDecoders() {
			@Override
			public Expression define(Class<?> valueClazz, Expression in, Expression method) {
				String methodName;
				for (int i = 1; ; i++) {
					methodName = "decode_" +
							valueClazz.getSimpleName().replace('[', 's').replace(']', '_') +
							(version == null ? "" : "_V" + version) +
							(i == 1 ? "" : "_" + i);
					String _methodName = methodName;
					if (classBuilder.getStaticMethods().keySet().stream().noneMatch(m -> m.getName().equals(_methodName)))
						break;
				}

				classBuilder.withStaticMethod(methodName, valueClazz, asList(BinaryInput.class), method);
				return staticCallSelf(methodName, in);
			}

			@Override
			public <T> ClassBuilder<T> buildClass(Class<T> type) {
				return ClassBuilder.create(classLoader, type);
			}
		};
	}

	@Nullable
	private Integer getLatestVersion(List<Integer> versions) {
		return versions.isEmpty() ? null : versions.get(versions.size() - 1);
	}

	private static final class Key {
		final Class<?> type;
		final SerializerForType[] generics;
		final List<SerializerDefBuilder> mods;

		private Key(@NotNull Class<?> type, @NotNull SerializerForType[] generics, @NotNull List<SerializerDefBuilder> mods) {
			this.type = type;
			this.generics = generics;
			this.mods = mods;
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
