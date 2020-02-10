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
import java.util.function.Function;

import static io.datakernel.codegen.Expressions.*;
import static io.datakernel.serializer.SerializerDef.StaticDecoders.IN;
import static io.datakernel.serializer.SerializerDef.StaticEncoders.*;
import static io.datakernel.serializer.impl.SerializerExpressions.*;
import static java.util.Collections.emptySet;

public abstract class AbstractSerializerDefMap implements SerializerDefWithNullable {
	protected final SerializerDef keySerializer;
	protected final SerializerDef valueSerializer;
	protected final Class<?> encodeType;
	protected final Class<?> decodeType;
	protected final Class<?> keyType;
	protected final Class<?> valueType;
	protected final boolean nullable;

	protected AbstractSerializerDefMap(@NotNull SerializerDef keySerializer, @NotNull SerializerDef valueSerializer, @NotNull Class<?> encodeType, @NotNull Class<?> decodeType, @NotNull Class<?> keyType, @NotNull Class<?> valueType, boolean nullable) {
		this.keySerializer = keySerializer;
		this.valueSerializer = valueSerializer;
		this.encodeType = encodeType;
		this.decodeType = decodeType;
		this.keyType = keyType;
		this.valueType = valueType;
		this.nullable = nullable;
	}

	protected abstract Expression mapForEach(Expression collection, Function<Expression, Expression> forEachKey, Function<Expression, Expression> forEachValue);

	protected Expression createConstructor(Expression length) {
		return constructor(decodeType, initialSize(length));
	}

	protected Expression initialSize(Expression length) {
		return div(mul(length, value(4)), value(3));
	}

	@Override
	public void accept(Visitor visitor) {
		visitor.visit("key", keySerializer);
		visitor.visit("value", valueSerializer);
	}

	@Override
	public Set<Integer> getVersions() {
		return emptySet();
	}

	@Override
	public Class<?> getEncodeType() {
		return encodeType;
	}

	@Override
	public Class<?> getDecodeType() {
		return decodeType;
	}

	@Override
	public final Expression defineEncoder(StaticEncoders staticEncoders, Expression buf, Variable pos, Expression value, int version, CompatibilityLevel compatibilityLevel) {
		return staticEncoders.define(encodeType, buf, pos, value,
				encoder(staticEncoders, BUF, POS, VALUE, version, compatibilityLevel));
	}

	@Override
	public Expression encoder(StaticEncoders staticEncoders, Expression buf, Variable pos, Expression value, int version, CompatibilityLevel compatibilityLevel) {
		Expression length = length(value);
		Expression writeLength = writeVarInt(buf, pos, !nullable ? length : inc(length));
		Expression forEach = mapForEach(value,
				k -> keySerializer.defineEncoder(staticEncoders, buf, pos, cast(k, keySerializer.getEncodeType()), version, compatibilityLevel),
				v -> valueSerializer.defineEncoder(staticEncoders, buf, pos, cast(v, valueSerializer.getEncodeType()), version, compatibilityLevel));

		return !nullable ?
				sequence(writeLength, forEach) :
				ifThenElse(isNull(value),
						writeByte(buf, pos, value((byte) 0)),
						sequence(writeLength, forEach));
	}

	@Override
	public final Expression defineDecoder(StaticDecoders staticDecoders, Expression in, int version, CompatibilityLevel compatibilityLevel) {
		return staticDecoders.define(getDecodeType(), in,
				decoder(staticDecoders, IN, version, compatibilityLevel));
	}

	@Override
	public Expression decoder(StaticDecoders staticDecoders, Expression in, int version, CompatibilityLevel compatibilityLevel) {
		return let(readVarInt(in), length ->
				!nullable ?
						let(createConstructor(length), instance -> sequence(
								loop(value(0), length,
										it -> sequence(
												call(instance, "put",
														cast(keySerializer.defineDecoder(staticDecoders, in, version, compatibilityLevel), keyType),
														cast(valueSerializer.defineDecoder(staticDecoders, in, version, compatibilityLevel), valueType)
												),
												voidExp())),
								instance)) :
						ifThenElse(
								cmpEq(length, value(0)),
								nullRef(decodeType),
								let(createConstructor(length), instance -> sequence(
										loop(value(0), dec(length),
												it -> sequence(
														call(instance, "put",
																cast(keySerializer.defineDecoder(staticDecoders, in, version, compatibilityLevel), keyType),
																cast(valueSerializer.defineDecoder(staticDecoders, in, version, compatibilityLevel), valueType)
														),
														voidExp())),
										instance))));
	}
}
