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

package io.datakernel.serializer.asm;

import io.datakernel.codegen.DefiningClassLoader;
import io.datakernel.codegen.Expression;
import io.datakernel.codegen.Variable;
import io.datakernel.serializer.CompatibilityLevel;
import io.datakernel.serializer.HasFixedSize;
import io.datakernel.serializer.HasNullable;
import org.jetbrains.annotations.NotNull;

import java.util.Objects;
import java.util.Set;

import static io.datakernel.codegen.Expressions.*;
import static io.datakernel.serializer.asm.SerializerDef.StaticDecoders.methodIn;
import static io.datakernel.serializer.asm.SerializerDef.StaticEncoders.*;
import static io.datakernel.serializer.asm.SerializerExpressions.*;
import static java.util.Collections.emptySet;

public final class SerializerDefArray implements SerializerDef, HasNullable, HasFixedSize {
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
	public SerializerDefArray withFixedSize(int fixedSize) {
		return new SerializerDefArray(valueSerializer, fixedSize, type, nullable);
	}

	@Override
	public SerializerDef withNullable() {
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
	public Class<?> getRawType() {
		return Object.class;
	}

	@Override
	public Expression encoder(DefiningClassLoader classLoader, StaticEncoders staticEncoders, Expression buf, Variable pos, Expression value, int version, CompatibilityLevel compatibilityLevel) {
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
			return staticEncoders.define(type, buf, pos, value,
					serializeArrayImpl(classLoader, staticEncoders, methodBuf(), methodPos(), methodValue(), version, compatibilityLevel));
		}
	}

	private Expression serializeArrayImpl(DefiningClassLoader classLoader, StaticEncoders staticEncoders, Expression buf, Variable pos, Expression value, int version, CompatibilityLevel compatibilityLevel) {
		Expression methodLength = fixedSize != -1 ? value(fixedSize) : length(cast(value, type));

		Expression writeCollection = loop(value(0), methodLength,
				it -> valueSerializer.encoder(classLoader, staticEncoders, buf, pos, getArrayItem(cast(value, type), it), version, compatibilityLevel));

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

	@Override
	public Expression decoder(DefiningClassLoader classLoader, StaticDecoders staticDecoders, Expression in, Class<?> targetType, int version, CompatibilityLevel compatibilityLevel) {
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
		} else {
			return staticDecoders.define(type, in,
					deserializeArrayImpl(classLoader, staticDecoders, methodIn(), version, compatibilityLevel));
		}
	}

	private Expression deserializeArrayImpl(DefiningClassLoader classLoader, StaticDecoders staticDecoders, Expression in, int version, CompatibilityLevel compatibilityLevel) {
		return !nullable ?
				let(readVarInt(in), len ->
						let(newArray(type, len), array ->
								sequence(
										loop(value(0), len,
												i -> setArrayItem(array, i,
														cast(valueSerializer.decoder(classLoader, staticDecoders, in, type.getComponentType(), version, compatibilityLevel), type.getComponentType()))),
										array))) :
				let(readVarInt(in), len ->
						ifThenElse(cmpEq(len, value(0)),
								nullRef(type),
								let(newArray(type, dec(len)), array ->
										sequence(
												loop(value(0), dec(len),
														i -> setArrayItem(array, i,
																cast(valueSerializer.decoder(classLoader, staticDecoders, in, type.getComponentType(), version, compatibilityLevel), type.getComponentType()))),
												array)
								)));
	}

	@SuppressWarnings("RedundantIfStatement")
	@Override
	public boolean equals(Object o) {
		if (this == o) return true;
		if (o == null || getClass() != o.getClass()) return false;

		SerializerDefArray that = (SerializerDefArray) o;

		if (fixedSize != that.fixedSize) return false;
		if (nullable != that.nullable) return false;
		if (!Objects.equals(valueSerializer, that.valueSerializer)) return false;
		if (!Objects.equals(type, that.type)) return false;
		return true;

	}

	@Override
	public int hashCode() {
		int result = valueSerializer != null ? valueSerializer.hashCode() : 0;
		result = 31 * result + fixedSize;
		result = 31 * result + (type != null ? type.hashCode() : 0);
		result = 31 * result + (nullable ? 1 : 0);
		return result;
	}

}
