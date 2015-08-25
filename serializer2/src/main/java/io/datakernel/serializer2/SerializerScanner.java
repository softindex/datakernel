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

package io.datakernel.serializer2;

import io.datakernel.asm.Annotations;
import io.datakernel.serializer2.annotations.*;
import io.datakernel.serializer2.asm.*;
import io.datakernel.serializer2.asm.SerializerGenBuilder.SerializerForType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.annotation.Annotation;
import java.lang.reflect.*;
import java.net.InetAddress;
import java.util.*;

import static io.datakernel.codegen.utils.Preconditions.check;
import static io.datakernel.codegen.utils.Preconditions.checkNotNull;
import static java.lang.reflect.Modifier.*;

/**
 * Scans fields of classes for serialization.
 */
public final class SerializerScanner {
	private static final Logger logger = LoggerFactory.getLogger(SerializerScanner.class);

	private String profile;

	private final Map<Class<?>, SerializerGenBuilder> typeMap = new LinkedHashMap<>();

	private final Map<Class<? extends Annotation>, Class<? extends Annotation>> annotationsExMap = new LinkedHashMap<>();
	private final Map<Class<? extends Annotation>, AnnotationHandler<?, ?>> annotationsMap = new LinkedHashMap<>();
	private final Map<String, Collection<Class<?>>> extraSubclassesMap = new HashMap<>();

	private SerializerScanner() {
	}

	public static SerializerScanner defaultScanner(String profile) {
		return defaultScanner().setProfile(profile);
	}

	/**
	 * Constructs a {@code SerializerScanner} that uses default settings.
	 *
	 * @return default serializer scanner
	 */
	public static SerializerScanner defaultScanner() {
		final SerializerScanner result = new SerializerScanner();

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
		result.register(InetAddress.class, SerializerGenInetAddress.instance());

		result.register(SerializeAscii.class, SerializeAsciiEx.class, new SerializeAsciiHandler());
		result.register(SerializerClass.class, SerializerClassEx.class, new SerializerClassHandler());
		result.register(SerializeUtf16.class, SerializeUtf16Ex.class, new SerializeUtf16Handler());
		result.register(SerializeFixedSize.class, SerializeFixedSizeEx.class, new SerializeFixedSizeHandler());
		result.register(SerializeVarLength.class, SerializeVarLengthEx.class, new SerializeVarLengthHandler());
		result.register(SerializeSubclasses.class, SerializeSubclassesEx.class, new SerializeSubclassesHandler());
		result.register(SerializeNullable.class, SerializeNullableEx.class, new SerializeNullableHandler());
		result.register(SerializeMaxLength.class, SerializeMaxLengthEx.class, new SerializeMaxLengthHandler());
		return result;
	}

	public static SerializerGen defaultSerializer(Class<?> type) {
		return defaultScanner().serializer(type);
	}

	public SerializerScanner setProfile(String profile) {
		this.profile = profile;
		return this;
	}

	public <A extends Annotation, P extends Annotation> SerializerScanner register(Class<A> annotation,
	                                                                               Class<P> annotationPlural,
	                                                                               AnnotationHandler<A, P> annotationHandler) {
		annotationsMap.put(annotation, annotationHandler);
		if (annotationPlural != null)
			annotationsExMap.put(annotation, annotationPlural);
		return this;
	}

	public SerializerScanner register(Class<?> type, SerializerGenBuilder serializer) {
		typeMap.put(type, serializer);
		return this;
	}

	public SerializerScanner register(Class<?> type, final SerializerGen serializer) {
		return register(type, new SerializerGenBuilderConst(serializer));
	}

	public SerializerScanner setExtraSubclasses(String extraSubclassesId, Collection<Class<?>> subclasses) {
		extraSubclassesMap.put(extraSubclassesId, subclasses);
		return this;
	}

	public SerializerScanner setExtraSubclasses(String extraSubclassesId, Class<?>... subclasses) {
		return setExtraSubclasses(extraSubclassesId, Arrays.asList(subclasses));
	}

	/**
	 * Creates a {@code SerializerGen} for the given type token.
	 *
	 * @return {@code SerializerGen} for the given type token
	 */
	public SerializerGen serializer(Class<?> type) {
		SerializerForType[] serializerForTypes = new SerializerForType[0];
		return serializer(type, serializerForTypes);
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

	public SerializerGen serializer(Class<?> type, SerializerForType[] generics) {
		return serializer(type, generics, Collections.<SerializerGenBuilder>emptyList());
	}

	private SerializerGen serializer(Class<?> type, SerializerForType[] generics, List<SerializerGenBuilder> mods) {
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

	private SerializerGen createSerializer(Class<?> type, SerializerForType[] generics, List<SerializerGenBuilder> mods) {
		if (!mods.isEmpty()) {
			SerializerGen serializer = serializer(type, generics, mods.subList(0, mods.size() - 1));
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

			SerializerGen serializer = serializer(subclass, new SerializerForType[]{});
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
					SerializerGenBuilder serializerGenBuilder = annotationHandler.createBuilder(this, annotation);
					TypedModsMap.Builder child = rootBuilder.ensureChild(annotationHandler.extractPath(annotation));
					child.add(serializerGenBuilder);
				}
			}
			for (Annotation annotationEx : annotations) {
				if (annotationEx.annotationType() == annotationExType) {
					for (Annotation annotation : annotationHandler.extractList(annotationEx)) {
						SerializerGenBuilder serializerGenBuilder = annotationHandler.createBuilder(this, annotation);
						TypedModsMap.Builder child = rootBuilder.ensureChild(annotationHandler.extractPath(annotation));
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
			check(i < classType.getTypeParameters().length);

			SerializerGen serializer = typedModsMap.rewrite(classGenerics[i].rawType, new SerializerForType[]{}, classGenerics[i].serializer);
			return new SerializerForType(classGenerics[i].rawType, serializer);
		} else if (genericType instanceof ParameterizedType) {
			ParameterizedType parameterizedType = (ParameterizedType) genericType;

			SerializerForType[] typeArguments = new SerializerForType[parameterizedType.getActualTypeArguments().length];
			for (int i = 0; i < parameterizedType.getActualTypeArguments().length; i++) {
				Type typeArgument = parameterizedType.getActualTypeArguments()[i];
				if (typeArgument instanceof WildcardType) {
					logger.error("Wildcard types not supported: {}, genericType={} ", classType, genericType);
					throw new IllegalArgumentException();
				}
				typeArguments[i] = resolveSerializer(classType, classGenerics, typeArgument, typedModsMap.get(i));
			}

			Class<?> rawType = (Class<?>) parameterizedType.getRawType();
			SerializerGen serializer = serializer(rawType, typeArguments, typedModsMap.getMods());
			return new SerializerForType(rawType, serializer);
		} else if (genericType instanceof GenericArrayType) {
			throw new UnsupportedOperationException(); // TODO
		} else if (genericType instanceof Class<?>) {
			Class<?> rawType = (Class<?>) genericType;
			SerializerForType[] generics = {};
			if (rawType.isArray()) {
				Class<?> componentType = rawType.getComponentType();
				SerializerForType forType = resolveSerializer(classType, classGenerics, componentType, typedModsMap.get(0));
				generics = new SerializerForType[]{forType};
			}
			SerializerGen serializer = serializer(rawType, generics, typedModsMap.getMods());
			return new SerializerForType(rawType, serializer);
		} else {
			logger.error("resolveSerializer error classType={}, classGenerics={}, genericType={}, typedModsMap={}", classType, classGenerics, genericType, typedModsMap);
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

	private FoundSerializer tryAddField(Class<?> classType, SerializerForType[] classGenerics, Field field) {
		FoundSerializer result = findAnnotations(field, field.getAnnotations());
		if (result == null)
			return null;
		check(isPublic(field.getModifiers()), "Field %s must be public", field);
		check(!isStatic(field.getModifiers()), "Field %s must not be static", field);
		check(!isTransient(field.getModifiers()), "Field %s must not be transient", field);
		result.serializerGen = resolveSerializer(classType, classGenerics, field.getGenericType(), result.mods).serializer;
		return result;
	}

	private FoundSerializer tryAddGetter(Class<?> classType, SerializerForType[] classGenerics, Method getter) {
		FoundSerializer result = findAnnotations(getter, getter.getAnnotations());
		if (result == null)
			return null;
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

	private void scanInterface(Class<?> classType, SerializerForType[] classGenerics, SerializerGenClass serializerGenClass, boolean inheritSerializers) {
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

	private void scanClass(Class<?> classType, SerializerForType[] classGenerics, SerializerGenClass serializerGenClass) {
		if (classType == Object.class)
			return;

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
		} else
			throw new IllegalArgumentException();

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
			if (foundSerializer != null)
				foundSerializers.add(foundSerializer);
		}
	}

	private void scanGetters(Class<?> classType, SerializerForType[] classGenerics, List<FoundSerializer> foundSerializers) {
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
}
