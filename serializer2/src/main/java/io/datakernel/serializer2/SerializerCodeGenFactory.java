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

import io.datakernel.codegen.AsmFunctionFactory;
import io.datakernel.codegen.FunctionDef;
import io.datakernel.codegen.utils.DefiningClassLoader;
import io.datakernel.codegen.utils.Preconditions;
import io.datakernel.serializer2.asm.SerializerGen;

import java.io.IOException;
import java.util.*;

import static io.datakernel.codegen.FunctionDefs.*;
import static io.datakernel.codegen.utils.Preconditions.checkNotNull;
import static java.lang.ClassLoader.getSystemClassLoader;
import static java.util.Arrays.asList;
import static org.objectweb.asm.Type.VOID_TYPE;
import static org.objectweb.asm.Type.getType;

/**
 * Contains static factory methods for various serializers.
 */
@SuppressWarnings("unchecked")
public class SerializerCodeGenFactory {
	private final ClassLoader rootClassLoader;
	private final Class<?> serializerClass;
	public static DefiningClassLoader definingClassLoader = new DefiningClassLoader();
	public static Map<SerializerGen, FunctionDef> map = new HashMap<>();

	private final boolean serialize;
	private final boolean deserialize;


	private final static class MethodKey {
		private MethodKey(SerializerGen serializerGen, int version) {
			this.serializerGen = checkNotNull(serializerGen);
			this.version = version;
		}

		private final SerializerGen serializerGen;
		private final int version;

		@Override
		public boolean equals(Object o) {
			if (this == o) return true;
			if (o == null || getClass() != o.getClass()) return false;

			MethodKey methodKey = (MethodKey) o;

			if (version != methodKey.version) return false;
			if (!serializerGen.equals(methodKey.serializerGen)) return false;

			return true;
		}

		@Override
		public int hashCode() {
			int result = serializerGen.hashCode();
			result = 31 * result + version;
			return result;
		}
	}

	private final static class MethodCall {
		private final String methodName;
		private final String dynamicClassName;

		private MethodCall(String methodName, String dynamicClassName) {
			this.methodName = methodName;
			this.dynamicClassName = dynamicClassName;
		}
	}

	private final Map<MethodKey, MethodCall> staticMethods = new HashMap<>();


	private SerializerCodeGenFactory(ClassLoader rootClassLoader, Class<?> serializerClass, Class<?> outputClass, Class<?> inputClass,
	                                 Class<? extends Exception> outputException, Class<? extends Exception> inputException, boolean serialize, boolean deserialize) {
		this.rootClassLoader = rootClassLoader;
		this.serialize = serialize;
		this.deserialize = deserialize;
		this.serializerClass = serializerClass;
		assert inputClass.isInterface() == outputClass.isInterface();
	}

	public ClassLoader getRootClassLoader() {
		return rootClassLoader;
	}

	/**
	 * Constructs a {@code SerializerFactory} that is able to instantiate serializers that work with buffers.
	 *
	 * @param rootClassLoader class loader to use for loading dynamic classes
	 * @param serialize       determines whether constructed factory can instantiate serializers
	 * @param deserialize     determines whether constructed factory can instantiate deserializers
	 * @return serializer factory that is able to instantiate serializers that work with buffers
	 */
	public static SerializerCodeGenFactory createBufferSerializerFactory(ClassLoader rootClassLoader, boolean serialize, boolean deserialize) {
		return new SerializerCodeGenFactory(rootClassLoader, BufferSerializer.class, SerializationOutputBuffer.class, SerializationInputBuffer.class, null, null, serialize, deserialize);
	}

	public static SerializerCodeGenFactory createBufferSerializerFactory() {
		return createBufferSerializerFactory(getSystemClassLoader(), true, true);
	}

	public static SerializerCodeGenFactory createStreamSerializerFactory(ClassLoader rootClassLoader, boolean serialize, boolean deserialize) {
		return new SerializerCodeGenFactory(rootClassLoader, StreamSerializer.class, SerializationOutputStream.class, SerializationInputStream.class, IOException.class, IOException.class, serialize, deserialize);
	}

	public static SerializerCodeGenFactory createStreamSerializerFactory() {
		return createStreamSerializerFactory(getSystemClassLoader(), true, true);
	}

	public <T> BufferSerializer<T> createBufferSerializer(SerializerGen serializerGen, int serializeVersion) {
		Preconditions.check(serializerClass == BufferSerializer.class);
		return (BufferSerializer<T>) createSerializer(serializerGen, serializeVersion);
	}

	/**
	 * Constructs buffer serializer for type, described by the given {@code SerializerGen}.
	 *
	 * @param serializerGen {@code SerializerGen} that describes the type that is to serialize
	 * @return buffer serializer for the given {@code SerializerGen}
	 */
	public <T> BufferSerializer<T> createBufferSerializer(SerializerGen serializerGen) {
		return createBufferSerializer(serializerGen, Integer.MAX_VALUE);
	}

	public <T> StreamSerializer<T> createStreamSerializer(SerializerGen serializerGen, int serializeVersion) {
		Preconditions.check(serializerClass == StreamSerializer.class);
		return (StreamSerializer<T>) createSerializer(serializerGen, serializeVersion);
	}

	public <T> StreamSerializer<T> createStreamSerializer(SerializerGen serializerGen) {
		return createStreamSerializer(serializerGen, Integer.MAX_VALUE);
	}

	public class SerializerStaticCallerStaticMethod implements SerializerStaticCaller {
		private AsmFunctionFactory<Object> factory = new AsmFunctionFactory<Object>(SerializerCodeGenFactory.definingClassLoader, Object.class);
		private int counter;

		public MethodCall ensureMethodCall(SerializerGen serializerGen, int version) {
			Class<?> dataType = serializerGen.getRawType();
			MethodKey methodKey = new MethodKey(serializerGen, version);
			MethodCall methodCall = staticMethods.get(methodKey);

			if (methodCall == null) {
				String methodName = dataType.getSimpleName().replace('[', 's').replace(']', '_') + "_V" + version + "_" + (++counter);
				methodCall = new MethodCall(methodName, factory.getClassName());
				staticMethods.put(methodKey, methodCall);

				if (serialize) {
					factory.staticMethod("serialize_" + methodName,
							void.class,
							asList(SerializationOutputBuffer.class, dataType),
							serializerGen.serialize(arg(1), serializerGen, version, this));
				}

				if (deserialize) {
					factory.staticMethod("deserialize_" + methodName,
							dataType,
							asList(SerializationInputBuffer.class),
							serializerGen.deserialize(dataType, version, this));
				}
			}
			return methodCall;
		}

		@Override
		public FunctionDef serialize(SerializerGen serializerGen, FunctionDef field, int version) {
			if (serializerGen.isInline()) {
				return serializerGen.serialize(field, serializerGen, version, this);
			}
			MethodCall methodCall = ensureMethodCall(serializerGen, version);
			return callFutureStatic(methodCall.dynamicClassName, "serialize_" + methodCall.methodName, VOID_TYPE, arg(0), field);
		}

		@Override
		public FunctionDef deserialize(SerializerGen serializerGen, int version, Class<?> targetType) {
			if (serializerGen.isInline()) {
				return serializerGen.deserialize(targetType, version, this);
			}
			MethodCall methodCall = ensureMethodCall(serializerGen, version);
			return callFutureStatic(methodCall.dynamicClassName, "deserialize_" + methodCall.methodName, getType(targetType), arg(0));
		}

		public void defineClass() {
			factory.defineClass();
		}
	}


	synchronized private Object createSerializer(SerializerGen serializerGen, int serializeVersion) {
		AsmFunctionFactory asmFactory = new AsmFunctionFactory(SerializerCodeGenFactory.definingClassLoader, BufferSerializer.class);

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

		FunctionDef version = voidFunc();
		if (currentVersion != null) {
			version = call(arg(0), "writeVarInt", value(currentVersion));
		}

		SerializerStaticCallerStaticMethod serializerCaller = new SerializerStaticCallerStaticMethod();

		if (currentVersion == null) currentVersion = 0;
		if (serialize) {
			asmFactory.method("serialize", sequence(version, serializerCaller.serialize(serializerGen, cast(arg(1), dataType), currentVersion)));
		}

		if (deserialize) {
			defineDeserialize(serializerGen, dataType, asmFactory, allVersions, serializerCaller);
		}
		asmFactory.defineClass();
		serializerCaller.defineClass();

		return asmFactory.newInstance();
	}

	private void defineDeserialize(final SerializerGen serializerGen, final Class<?> dataType, final AsmFunctionFactory asmFactory, final List<Integer> allVersions, SerializerStaticCaller serializerCaller) {
		defineDeserializeLatest(serializerGen, dataType, asmFactory, getLatestVersion(allVersions), serializerCaller);

		defineDeserializeEarlierVersion(dataType, asmFactory, allVersions, serializerCaller);
		for (int i = allVersions.size() - 2; i >= 0; i--) {
			int version = allVersions.get(i);
			defineDeserializeVersion(serializerGen, dataType, asmFactory, version, serializerCaller);
		}
	}

	private void defineDeserializeVersion(SerializerGen serializerGen, Class<?> dataType, AsmFunctionFactory asmFactory, int version, SerializerStaticCaller serializerCaller) {
		asmFactory.method("deserializeVersion" + String.valueOf(version), dataType, asList(SerializationInputBuffer.class), sequence(serializerCaller.deserialize(serializerGen, version, dataType)));
	}

	private void defineDeserializeEarlierVersion(Class<?> dataType, AsmFunctionFactory asmFactory, List<Integer> allVersions, SerializerStaticCaller serializerCaller) {
		List<FunctionDef> listKey = new ArrayList<>();
		List<FunctionDef> listValue = new ArrayList<>();
		for (int i = allVersions.size() - 2; i >= 0; i--) {
			int version = allVersions.get(i);
			listKey.add(value(version));
			listValue.add(callFuture(asmFactory.getClassName(), "deserializeVersion" + String.valueOf(version), getType(dataType), arg(0)));
		}
		asmFactory.method("deserializeEarlierVersions", dataType, asList(SerializationInputBuffer.class, int.class),
				switchForKey(arg(1), getType(dataType), listKey, listValue));
	}

	private void defineDeserializeLatest(final SerializerGen serializerGen, final Class<?> dataType, final AsmFunctionFactory asmFactory, final Integer latestVersion, final SerializerStaticCaller serializerCaller) {
		if (latestVersion == null) {
			asmFactory.method("deserialize", serializerCaller.deserialize(serializerGen, 0, dataType));
		} else {
			FunctionDef version = let(call(arg(0), "readVarInt"));
			asmFactory.method("deserialize", choice(cmpEq(version, value(latestVersion)),
					serializerGen.deserialize(dataType, latestVersion, serializerCaller),
					callFuture(asmFactory.getClassName(), "deserializeEarlierVersions", getType(dataType), arg(0), version)));
		}
	}

	private Integer getLatestVersion(List<Integer> versions) {
		return versions.isEmpty() ? null : versions.get(versions.size() - 1);
	}
}
