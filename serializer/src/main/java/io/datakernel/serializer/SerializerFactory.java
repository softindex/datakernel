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

import io.datakernel.codegen.AsmFunctionFactory;
import io.datakernel.codegen.FunctionDef;
import io.datakernel.codegen.utils.DefiningClassLoader;
import io.datakernel.codegen.utils.Preconditions;
import io.datakernel.serializer.asm.SerializerGen;

import java.util.*;

import static io.datakernel.codegen.FunctionDefs.*;
import static java.lang.ClassLoader.getSystemClassLoader;
import static java.util.Arrays.asList;

/**
 * Contains static factory methods for various serializers.
 */
@SuppressWarnings("unchecked")
public class SerializerFactory {
	private final ClassLoader rootClassLoader;
	private final Class<?> serializerClass;
	private final DefiningClassLoader definingClassLoader;

	private final boolean serialize;
	private final boolean deserialize;

	private SerializerFactory(ClassLoader rootClassLoader, Class<?> serializerClass, Class<?> outputClass, Class<?> inputClass,
	                          boolean serialize, boolean deserialize) {
		this.rootClassLoader = rootClassLoader;
		this.definingClassLoader = new DefiningClassLoader(rootClassLoader);
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
	public static SerializerFactory createBufferSerializerFactory(ClassLoader rootClassLoader, boolean serialize, boolean deserialize) {
		return new SerializerFactory(rootClassLoader, BufferSerializer.class, SerializationOutputBuffer.class, SerializationInputBuffer.class, serialize, deserialize);
	}

	public static SerializerFactory createBufferSerializerFactory() {
		return createBufferSerializerFactory(getSystemClassLoader(), true, true);
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

	public class StaticMethods {
		private final DefiningClassLoader definingClassLoader = SerializerFactory.this.definingClassLoader;
		private int counter = 0;

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
			public FunctionDef functionDef;

			public Value(String method, FunctionDef functionDef) {
				this.method = method;
				this.functionDef = functionDef;
			}
		}

		private Map<Key, Value> mapSerialize = new HashMap<>();
		private Map<Key, Value> mapDeserialize = new HashMap<>();

		public boolean startSerializeStaticMethod(SerializerGen serializerGen, int version) {
			boolean b = mapSerialize.containsKey(new Key(serializerGen, version));
			if (!b) {
				String methodName = "serialize_" + serializerGen.getRawType().getSimpleName().replace('[', 's').replace(']', '_') + "_V" + version + "_" + (++counter);
				mapSerialize.put(new Key(serializerGen, version), new Value(methodName, null));
			}
			return b;
		}

		public boolean startDeserializeStaticMethod(SerializerGen serializerGen, int version) {
			boolean b = mapDeserialize.containsKey(new Key(serializerGen, version));
			if (!b) {
				String methodName = "deserialize_" + serializerGen.getRawType().getSimpleName().replace('[', 's').replace(']', '_') + "_V" + version + "_" + (++counter);
				mapDeserialize.put(new Key(serializerGen, version), new Value(methodName, null));
			}
			return b;
		}

		public void registerStaticSerializeMethod(SerializerGen serializerGen, int version, FunctionDef functionDef) {
			Key key = new Key(serializerGen, version);
			Value value = mapSerialize.get(key);
			value.functionDef = functionDef;
		}

		public void registerStaticDeserializeMethod(SerializerGen serializerGen, int version, FunctionDef functionDef) {
			Key key = new Key(serializerGen, version);
			Value value = mapDeserialize.get(key);
			value.functionDef = functionDef;
		}

		public FunctionDef callStaticSerializeMethod(SerializerGen serializerGen, int version, FunctionDef... args) {
			Value value = mapSerialize.get(new Key(serializerGen, version));
			return callStaticSelf(value.method, args);
		}

		public FunctionDef callStaticDeserializeMethod(SerializerGen serializerGen, int version, FunctionDef... args) {
			Value value = mapDeserialize.get(new Key(serializerGen, version));
			return callStaticSelf(value.method, args);
		}
	}

	synchronized private Object createSerializer(SerializerGen serializerGen, int serializeVersion) {
		AsmFunctionFactory asmFactory = new AsmFunctionFactory(definingClassLoader, BufferSerializer.class);

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

		StaticMethods staticMethods = new StaticMethods();

		if (currentVersion == null) currentVersion = 0;
		if (serialize) {
			serializerGen.prepareSerializeStaticMethods(currentVersion, staticMethods);
			for (StaticMethods.Key key : staticMethods.mapSerialize.keySet()) {
				StaticMethods.Value value = staticMethods.mapSerialize.get(key);
				asmFactory.staticMethod(value.method,
						void.class,
						asList(SerializationOutputBuffer.class, key.serializerGen.getRawType()),
						value.functionDef);
			}
			asmFactory.method("serialize", sequence(version, serializerGen.serialize(cast(arg(1), dataType), currentVersion, staticMethods)));
		}

		if (deserialize) {
			defineDeserialize(serializerGen, asmFactory, allVersions, staticMethods);
			for (StaticMethods.Key key : staticMethods.mapDeserialize.keySet()) {
				StaticMethods.Value value = staticMethods.mapDeserialize.get(key);
				asmFactory.staticMethod(value.method,
						key.serializerGen.getRawType(),
						asList(SerializationInputBuffer.class),
						value.functionDef);
			}
		}

		return asmFactory.newInstance();
	}

	private void defineDeserialize(final SerializerGen serializerGen, final AsmFunctionFactory asmFactory, final List<Integer> allVersions, StaticMethods staticMethods) {
		defineDeserializeLatest(serializerGen, asmFactory, getLatestVersion(allVersions), staticMethods);

		defineDeserializeEarlierVersion(serializerGen, asmFactory, allVersions, staticMethods);
		for (int i = allVersions.size() - 2; i >= 0; i--) {
			int version = allVersions.get(i);
			defineDeserializeVersion(serializerGen, asmFactory, version, staticMethods);
		}
	}

	private void defineDeserializeVersion(SerializerGen serializerGen, AsmFunctionFactory asmFactory, int version, StaticMethods staticMethods) {
		asmFactory.method("deserializeVersion" + String.valueOf(version), serializerGen.getRawType(), asList(SerializationInputBuffer.class), sequence(serializerGen.deserialize(serializerGen.getRawType(), version, staticMethods)));
	}

	private void defineDeserializeEarlierVersion(SerializerGen serializerGen, AsmFunctionFactory asmFactory, List<Integer> allVersions, StaticMethods staticMethods) {
		List<FunctionDef> listKey = new ArrayList<>();
		List<FunctionDef> listValue = new ArrayList<>();
		for (int i = allVersions.size() - 2; i >= 0; i--) {
			int version = allVersions.get(i);
			listKey.add(value(version));
			serializerGen.prepareDeserializeStaticMethods(version, staticMethods);
			listValue.add(call(self(), "deserializeVersion" + String.valueOf(version), arg(0)));
		}
		asmFactory.method("deserializeEarlierVersions", serializerGen.getRawType(), asList(SerializationInputBuffer.class, int.class),
				switchForKey(arg(1), listKey, listValue));
	}

	private void defineDeserializeLatest(final SerializerGen serializerGen, final AsmFunctionFactory asmFactory, final Integer latestVersion, StaticMethods staticMethods) {
		if (latestVersion == null) {
			serializerGen.prepareDeserializeStaticMethods(0, staticMethods);
			asmFactory.method("deserialize", serializerGen.deserialize(serializerGen.getRawType(), 0, staticMethods));
		} else {
			serializerGen.prepareDeserializeStaticMethods(latestVersion, staticMethods);
			FunctionDef version = let(call(arg(0), "readVarInt"));
			asmFactory.method("deserialize", sequence(version, choice(cmpEq(version, value(latestVersion)),
					serializerGen.deserialize(serializerGen.getRawType(), latestVersion, staticMethods),
					call(self(), "deserializeEarlierVersions", arg(0), version))));
		}
	}

	private Integer getLatestVersion(List<Integer> versions) {
		return versions.isEmpty() ? null : versions.get(versions.size() - 1);
	}
}
