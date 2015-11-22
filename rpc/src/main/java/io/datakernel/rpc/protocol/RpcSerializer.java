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

package io.datakernel.rpc.protocol;

import io.datakernel.serializer.BufferSerializer;
import io.datakernel.serializer.SerializerBuilder;
import io.datakernel.serializer.asm.SerializerGen;
import io.datakernel.serializer.asm.SerializerGenBuilder;
import io.datakernel.serializer.asm.SerializerGenBuilderConst;

import java.util.*;
import java.util.Map.Entry;

import static io.datakernel.util.Preconditions.checkNotNull;

public final class RpcSerializer {

	private ClassLoader classLoader = ClassLoader.getSystemClassLoader();
	private final Set<Class<?>> extraSubClasses = new LinkedHashSet<>();
	private final Map<Class<?>, SerializerGenBuilder> extraSerializers = new LinkedHashMap<>();
	private int serializeVersion;

	private RpcSerializer() {
	}

	public static RpcSerializer serializerFor() {
		return new RpcSerializer();
	}

	public static RpcSerializer serializerFor(Class<?>... messageTypes) {
		return serializerFor().messageTypes(messageTypes);
	}

	public RpcSerializer setClassLoader(ClassLoader classLoader) {
		this.classLoader = classLoader;
		return this;
	}

	public RpcSerializer addSerializer(Class<?> type, SerializerGenBuilder serializer) {
		extraSerializers.put(type, checkNotNull(serializer));
		return this;
	}

	public RpcSerializer addSerializer(Class<?> type, SerializerGen serializer) {
		return addSerializer(type, new SerializerGenBuilderConst(checkNotNull(serializer)));
	}

	public RpcSerializer messageTypes(Class<?>... messageTypes) {
		extraSubClasses.addAll(Arrays.asList(messageTypes));
		return this;
	}

	public RpcSerializer setSerializeVersion(int serializeVersion) {
		this.serializeVersion = serializeVersion;
		return this;
	}

	public BufferSerializer<RpcMessage> createSerializer() {
		SerializerBuilder serializerBuilder = SerializerBuilder.newDefaultInstance(classLoader);
		for (Entry<Class<?>, SerializerGenBuilder> serializer : extraSerializers.entrySet())
			serializerBuilder.registry(serializer.getKey(), serializer.getValue());
		serializerBuilder.setExtraSubclasses("extraRpcMessageData", extraSubClasses);
		if (serializeVersion == 0)
			return serializerBuilder.create(RpcMessage.class);
		else
			return serializerBuilder.version(serializeVersion).create(RpcMessage.class);
	}

}
