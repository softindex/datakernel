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

package io.datakernel.serializer.impl;

import io.datakernel.codegen.Expression;
import io.datakernel.codegen.Variable;
import io.datakernel.serializer.CompatibilityLevel;
import io.datakernel.serializer.SerializerDef;
import org.jetbrains.annotations.NotNull;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Set;

import static io.datakernel.codegen.Expressions.*;
import static io.datakernel.common.Utils.of;
import static io.datakernel.serializer.SerializerDef.StaticDecoders.IN;
import static io.datakernel.serializer.SerializerDef.StaticEncoders.*;
import static io.datakernel.serializer.impl.SerializerExpressions.readByte;
import static io.datakernel.serializer.impl.SerializerExpressions.writeByte;
import static java.util.Collections.emptySet;
import static org.objectweb.asm.Type.getType;

public final class SerializerDefSubclass implements SerializerDefWithNullable {
	private final Class<?> dataType;
	private final LinkedHashMap<Class<?>, SerializerDef> subclassSerializers;
	private final boolean nullable;
	private final int startIndex;

	public SerializerDefSubclass(@NotNull Class<?> dataType, LinkedHashMap<Class<?>, SerializerDef> subclassSerializers, int startIndex) {
		this.startIndex = startIndex;
		this.dataType = dataType;
		this.subclassSerializers = new LinkedHashMap<>(subclassSerializers);
		this.nullable = false;
	}

	private SerializerDefSubclass(@NotNull Class<?> dataType, LinkedHashMap<Class<?>, SerializerDef> subclassSerializers, boolean nullable, int startIndex) {
		this.startIndex = startIndex;
		this.dataType = dataType;
		this.subclassSerializers = new LinkedHashMap<>(subclassSerializers);
		this.nullable = nullable;
	}

	@Override
	public SerializerDef ensureNullable() {
		return new SerializerDefSubclass(dataType, subclassSerializers, true, startIndex);
	}

	@Override
	public void accept(Visitor visitor) {
		for (Class<?> subclass : subclassSerializers.keySet()) {
			visitor.visit(subclass.getName(), subclassSerializers.get(subclass));
		}
	}

	@Override
	public Set<Integer> getVersions() {
		return emptySet();
	}

	@Override
	public Class<?> getEncodeType() {
		return dataType;
	}

	@Override
	public Expression defineEncoder(StaticEncoders staticEncoders, Expression buf, Variable pos, Expression value, int version, CompatibilityLevel compatibilityLevel) {
		return staticEncoders.define(dataType, buf, pos, value,
				encoder(staticEncoders, BUF, POS, VALUE, version, compatibilityLevel));
	}

	@Override
	public Expression encoder(StaticEncoders staticEncoders, Expression buf, Variable pos, Expression value, int version, CompatibilityLevel compatibilityLevel) {
		int subClassIndex = (nullable && startIndex == 0 ? 1 : startIndex);

		List<Expression> listKey = new ArrayList<>();
		List<Expression> listValue = new ArrayList<>();
		for (Class<?> subclass : subclassSerializers.keySet()) {
			SerializerDef subclassSerializer = subclassSerializers.get(subclass);
			listKey.add(cast(value(getType(subclass)), Object.class));
			listValue.add(sequence(
					writeByte(buf, pos, value((byte) subClassIndex)),
					subclassSerializer.defineEncoder(staticEncoders, buf, pos, cast(value, subclassSerializer.getEncodeType()), version, compatibilityLevel)
			));

			subClassIndex++;
			if (nullable && subClassIndex == 0) {
				subClassIndex++;
			}
		}
		if (nullable) {
			return ifThenElse(isNotNull(value),
					switchByKey(call(value, "getClass"), listKey, listValue),
					writeByte(buf, pos, value((byte) 0)));
		} else {
			return switchByKey(call(value, "getClass"), listKey, listValue);
		}
	}

	@Override
	public Expression defineDecoder(StaticDecoders staticDecoders, Expression in, int version, CompatibilityLevel compatibilityLevel) {
		return staticDecoders.define(getDecodeType(), in,
				decoder(staticDecoders, IN, version, compatibilityLevel));

	}

	@Override
	public Expression decoder(StaticDecoders staticDecoders, Expression in, int version, CompatibilityLevel compatibilityLevel) {
		return let(startIndex != 0 ? sub(readByte(in), value(startIndex)) : cast(readByte(in), int.class),
				idx -> cast(
						switchByIndex(idx,
								of(() -> {
									List<Expression> versions = new ArrayList<>();
									for (SerializerDef subclassSerializer : subclassSerializers.values()) {
										versions.add(cast(subclassSerializer.defineDecoder(staticDecoders, in, version, compatibilityLevel), dataType));
									}
									if (nullable) versions.add(-startIndex, nullRef(getDecodeType()));
									return versions;
								})),
						dataType));
	}
}
