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
import static io.datakernel.util.Preconditions.checkState;

public final class RpcMessageSerializer {

	public static class Builder {
		private final Set<Class<?>> extraSubClasses = new LinkedHashSet<>();
		private final Map<Class<?>, SerializerGenBuilder> extraSerializers = new LinkedHashMap<>();
		private int serializeVersion;

		private Builder() {
		}

		public Builder addSerializer(Class<?> type, SerializerGenBuilder serializer) {
			extraSerializers.put(type, checkNotNull(serializer));
			return this;
		}

		public Builder addSerializer(Class<?> type, final SerializerGen serializer) {
			return addSerializer(type, new SerializerGenBuilderConst(checkNotNull(serializer)));
		}

		@SafeVarargs
		public final Builder addExtraRpcMessageType(Class<? extends Object>... subclasses) {
			extraSubClasses.addAll(Arrays.asList(subclasses));
			return this;
		}

		public Builder setSerializeVersion(int serializeVersion) {
			this.serializeVersion = serializeVersion;
			return this;
		}

		public RpcMessageSerializer build() {
			checkState(!extraSubClasses.isEmpty());
			return new RpcMessageSerializer(this);
		}
	}

	public static Builder builder() {
		return new Builder();
	}

	private final BufferSerializer<RpcMessage> messageSerializer;

	private RpcMessageSerializer(Builder builder) {
		SerializerBuilder serializersRegistry = SerializerBuilder.newDefaultInstance(ClassLoader.getSystemClassLoader());
		for (Entry<Class<?>, SerializerGenBuilder> serializer : builder.extraSerializers.entrySet())
			serializersRegistry.registry(serializer.getKey(), serializer.getValue());
		serializersRegistry.setExtraSubclasses("extraRpcMessageData", builder.extraSubClasses);
		if (builder.serializeVersion == 0)
			this.messageSerializer = serializersRegistry.create(RpcMessage.class);
		else
			this.messageSerializer = serializersRegistry.version(builder.serializeVersion).create(RpcMessage.class);
	}

	public BufferSerializer<RpcMessage> getSerializer() {
		return messageSerializer;
	}
}
