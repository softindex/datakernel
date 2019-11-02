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

import java.util.Set;

import static io.datakernel.codegen.Expressions.*;
import static io.datakernel.serializer.SerializerDef.StaticDecoders.methodIn;
import static io.datakernel.serializer.SerializerDef.StaticEncoders.*;
import static io.datakernel.serializer.impl.SerializerExpressions.*;
import static java.util.Collections.emptySet;

public final class SerializerDefArray implements SerializerDefWithNullable, SerializerDefWithFixedSize {
	private final SerializerDef valueSerializer;
	private final int fixedSize;
	private final Class<?> type;
	private final boolean nullable;

	public SerializerDefArray(SerializerDef serializer, Class<?> type) {
		this.valueSerializer = serializer;
		this.fixedSize = -1;
		this.type = type;
		this.nullable = false;
	}

	private SerializerDefArray(@NotNull SerializerDef serializer, int fixedSize, Class<?> type, boolean nullable) {
		this.valueSerializer = serializer;
		this.fixedSize = fixedSize;
		this.type = type;
		this.nullable = nullable;
	}

	@Override
	public SerializerDefArray ensureFixedSize(int fixedSize) {
		return new SerializerDefArray(valueSerializer, fixedSize, type, nullable);
	}

	@Override
	public SerializerDef ensureNullable() {
		return new SerializerDefArray(valueSerializer, fixedSize, type, true);
	}

	@Override
	public void accept(Visitor visitor) {
		visitor.visit(valueSerializer);
	}

	@Override
	public Set<Integer> getVersions() {
		return emptySet();
	}

	@Override
	public Class<?> getEncodeType() {
		return Object.class;
	}

	@Override
	public Expression defineEncoder(StaticEncoders staticEncoders, Expression buf, Variable pos, Expression value, int version, CompatibilityLevel compatibilityLevel) {
		if (type.getComponentType() == Byte.TYPE) {
			return encoder(staticEncoders, buf, pos, value, version, compatibilityLevel);
		} else {
			return staticEncoders.define(type, buf, pos, value,
					encoder(staticEncoders, methodBuf(), methodPos(), methodValue(), version, compatibilityLevel));
		}
	}

	@Override
	public Expression encoder(StaticEncoders staticEncoders, Expression buf, Variable pos, Expression value, int version, CompatibilityLevel compatibilityLevel) {
		if (type.getComponentType() == Byte.TYPE) {
			Expression castedValue = cast(value, type);
			Expression length = fixedSize != -1 ? value(fixedSize) : length(castedValue);

			if (!nullable) {
				return sequence(
						writeVarInt(buf, pos, length),
						writeBytes(buf, pos, castedValue));
			} else {
				return ifThenElse(isNull(value),
						writeByte(buf, pos, value((byte) 0)),
						sequence(
								writeVarInt(buf, pos, inc(length)),
								writeBytes(buf, pos, castedValue))
				);
			}
		} else {
			Expression methodLength = fixedSize != -1 ? value(fixedSize) : length(cast(value, type));

			Expression writeCollection = loop(value(0), methodLength,
					it -> valueSerializer.defineEncoder(staticEncoders, buf, pos, getArrayItem(cast(value, type), it), version, compatibilityLevel));

			if (!nullable) {
				return sequence(
						writeVarInt(buf, pos, methodLength),
						writeCollection);
			} else {
				return ifThenElse(isNull(value),
						writeByte(buf, pos, value((byte) 0)),
						sequence(
								writeVarInt(buf, pos, inc(methodLength)),
								writeCollection));
			}
		}
	}

	@Override
	public Expression defineDecoder(StaticDecoders staticDecoders, Expression in, int version, CompatibilityLevel compatibilityLevel) {
		if (type.getComponentType() == Byte.TYPE) {
			return decoder(staticDecoders, in, version, compatibilityLevel);
		} else {
			return staticDecoders.define(getDecodeType(), in,
					decoder(staticDecoders, methodIn(), version, compatibilityLevel));
		}
	}

	@Override
	public Expression decoder(StaticDecoders staticDecoders, Expression in, int version, CompatibilityLevel compatibilityLevel) {
		if (type.getComponentType() == Byte.TYPE) {
			return !nullable ?
					let(readVarInt(in), len ->
							let(newArray(type, len), array ->
									sequence(
											readBytes(in, array),
											array))) :
					let(readVarInt(in), len ->
							ifThenElse(cmpEq(len, value(0)),
									nullRef(type),
									let(newArray(type, dec(len)), array ->
											sequence(
													readBytes(in, array),
													array)
									)));
		}

		return !nullable ?
				let(readVarInt(in), len ->
						let(newArray(type, len), array ->
								sequence(
										loop(value(0), len,
												i -> setArrayItem(array, i,
														cast(valueSerializer.defineDecoder(staticDecoders, in, version, compatibilityLevel), type.getComponentType()))),
										array))) :
				let(readVarInt(in), len ->
						ifThenElse(cmpEq(len, value(0)),
								nullRef(type),
								let(newArray(type, dec(len)), array ->
										sequence(
												loop(value(0), dec(len),
														i -> setArrayItem(array, i,
																cast(valueSerializer.defineDecoder(staticDecoders, in, version, compatibilityLevel), type.getComponentType()))),
												array)
								)));
	}
}
