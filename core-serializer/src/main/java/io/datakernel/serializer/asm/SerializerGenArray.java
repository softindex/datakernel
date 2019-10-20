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
import static io.datakernel.serializer.asm.SerializerExpressions.*;
import static java.util.Collections.emptySet;

public final class SerializerGenArray implements SerializerGen, HasNullable, HasFixedSize {
	private final SerializerGen valueSerializer;
	private final int fixedSize;
	private final Class<?> type;
	private final boolean nullable;

	public SerializerGenArray(SerializerGen serializer, Class<?> type) {
		this.valueSerializer = serializer;
		this.fixedSize = -1;
		this.type = type;
		this.nullable = false;
	}

	private SerializerGenArray(@NotNull SerializerGen serializer, int fixedSize, Class<?> type, boolean nullable) {
		this.valueSerializer = serializer;
		this.fixedSize = fixedSize;
		this.type = type;
		this.nullable = nullable;
	}

	@Override
	public SerializerGenArray withFixedSize(int fixedSize) {
		return new SerializerGenArray(valueSerializer, fixedSize, type, nullable);
	}

	@Override
	public SerializerGen withNullable() {
		return new SerializerGenArray(valueSerializer, fixedSize, type, true);
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
	public boolean isInline() {
		return true;
	}

	@Override
	public Class<?> getRawType() {
		return Object.class;
	}

	@Override
	public Expression serialize(DefiningClassLoader classLoader, Expression buf, Variable pos, Expression value, int version,
			CompatibilityLevel compatibilityLevel) {
		Expression castedValue = cast(value, type);
		Expression length = fixedSize != -1 ? value(fixedSize) : length(castedValue);
		Expression writeLength = writeVarInt(buf, pos, (!nullable ? length : inc(length)));
		Expression writeZeroLength = writeByte(buf, pos, value((byte) 0));
		Expression writeByteArray = writeBytes(buf, pos, castedValue);
		Expression writeCollection = loop(value(0), length,
				it -> valueSerializer.serialize(classLoader, buf, pos, getArrayItem(castedValue, it), version, compatibilityLevel));

		if (!nullable) {
			return type.getComponentType() == Byte.TYPE ?
					sequence(writeLength, writeByteArray) :
					sequence(writeLength, writeCollection);
		} else {
			return type.getComponentType() == Byte.TYPE ?
					ifThenElse(isNull(value),
							writeZeroLength,
							sequence(writeLength, writeByteArray)
					) :
					ifThenElse(isNull(value),
							writeZeroLength,
							sequence(writeLength, writeCollection));
		}
	}

	@Override
	public Expression deserialize(DefiningClassLoader classLoader, Expression in, Class<?> targetType, int version, CompatibilityLevel compatibilityLevel) {
		return !nullable ?
				let(readVarInt(in), len ->
						let(newArray(type, len), array ->
								sequence(
										type.getComponentType() == Byte.TYPE ?
												readBytes(in, array) :
												loop(value(0), len,
														i -> setArrayItem(array, i,
																cast(valueSerializer.deserialize(classLoader, in, type.getComponentType(), version, compatibilityLevel), type.getComponentType()))),
										array))) :
				let(readVarInt(in), len ->
						ifThenElse(cmpEq(len, value(0)),
								nullRef(type),
								let(newArray(type, dec(len)), array ->
										sequence(
												type.getComponentType() == Byte.TYPE ?
														readBytes(in, array) :
														loop(value(0), dec(len),
																i -> setArrayItem(array, i,
																		cast(valueSerializer.deserialize(classLoader, in, type.getComponentType(), version, compatibilityLevel), type.getComponentType()))),
												array)
								)));
	}

	@SuppressWarnings("RedundantIfStatement")
	@Override
	public boolean equals(Object o) {
		if (this == o) return true;
		if (o == null || getClass() != o.getClass()) return false;

		SerializerGenArray that = (SerializerGenArray) o;

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
