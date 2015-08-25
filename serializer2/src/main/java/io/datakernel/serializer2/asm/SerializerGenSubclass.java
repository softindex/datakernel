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

package io.datakernel.serializer2.asm;

import io.datakernel.codegen.FunctionDef;
import io.datakernel.codegen.utils.Preconditions;
import io.datakernel.serializer2.SerializerStaticCaller;

import java.util.*;

import static io.datakernel.codegen.FunctionDefs.*;
import static io.datakernel.codegen.utils.Preconditions.checkNotNull;
import static org.objectweb.asm.Type.VOID_TYPE;
import static org.objectweb.asm.Type.getType;

@SuppressWarnings("PointlessArithmeticExpression")
public class SerializerGenSubclass implements SerializerGen {
	public static final class Builder {
		private final Class<?> dataType;
		private final LinkedHashMap<Class<?>, SerializerGen> subclassSerializers = new LinkedHashMap<>();

		public Builder(Class<?> dataType) {
			this.dataType = dataType;
		}

		public Builder add(Class<?> subclass, SerializerGen serializer) {
			Preconditions.check(subclassSerializers.put(subclass, serializer) == null);
			return this;
		}

		public SerializerGenSubclass build() {
			return new SerializerGenSubclass(dataType, subclassSerializers);
		}
	}

	private final Class<?> dataType;
	private final Map<Class<?>, SerializerGen> subclassSerializers;

	public SerializerGenSubclass(Class<?> dataType, LinkedHashMap<Class<?>, SerializerGen> subclassSerializers) {
		this.dataType = checkNotNull(dataType);
		this.subclassSerializers = new HashMap<>(subclassSerializers);
	}

	@Override
	public void getVersions(VersionsCollector versions) {
		for (SerializerGen serializer : subclassSerializers.values()) {
			versions.addRecursive(serializer);
		}
	}

	@Override
	public boolean isInline() {
		return false;
	}

	@Override
	public Class<?> getRawType() {
		return dataType;
	}

	@Override
	public FunctionDef serialize(FunctionDef field, SerializerGen serializerGen, int version, SerializerStaticCaller serializerCaller) {
		byte subClassN = 0;
		List<FunctionDef> listKey = new ArrayList<>();
		List<FunctionDef> listValue = new ArrayList<>();
		for (Class<?> subclass : subclassSerializers.keySet()) {
			SerializerGen subclassSerializer = subclassSerializers.get(subclass);
			listKey.add(value(subclass.getName()));
			listValue.add(sequence(
					call(arg(0), "writeByte", value(subClassN++)),
					serializerCaller.serialize(subclassSerializer, cast(field, subclassSerializer.getRawType()), version)));
		}
		return switchForKey(cast(call(call(cast(field, Object.class), "getClass"), "getName"), Object.class), VOID_TYPE, listKey, listValue);
	}

	@Override
	public FunctionDef deserialize(Class<?> targetType, int version, SerializerStaticCaller serializerCaller) {
		List<FunctionDef> list = new ArrayList<>();
		for (Class<?> subclass : subclassSerializers.keySet()) {
			SerializerGen subclassSerializer = subclassSerializers.get(subclass);
			list.add(serializerCaller.deserialize(subclassSerializer, version, subclassSerializer.getRawType()));
		}

		return cast(switchForPosition(call(arg(0), "readByte"), getType(targetType), list), targetType);
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) return true;
		if (o == null || getClass() != o.getClass()) return false;

		SerializerGenSubclass that = (SerializerGenSubclass) o;

		if (!dataType.equals(that.dataType)) return false;
		if (!subclassSerializers.equals(that.subclassSerializers)) return false;

		return true;
	}

	@Override
	public int hashCode() {
		int result = dataType.hashCode();
		result = 31 * result + subclassSerializers.hashCode();
		return result;
	}
}
