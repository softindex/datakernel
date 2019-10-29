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
import io.datakernel.serializer.HasNullable;
import org.jetbrains.annotations.NotNull;

import java.util.Set;
import java.util.function.Function;

import static io.datakernel.codegen.Expressions.*;
import static io.datakernel.common.Preconditions.checkArgument;
import static io.datakernel.serializer.asm.SerializerDef.StaticDecoders.methodIn;
import static io.datakernel.serializer.asm.SerializerDef.StaticEncoders.*;
import static io.datakernel.serializer.asm.SerializerExpressions.*;
import static java.util.Collections.emptySet;

public abstract class AbstractSerializerDefMap implements SerializerDef, HasNullable {
	protected final SerializerDef keySerializer;
	protected final SerializerDef valueSerializer;
	protected final Class<?> mapType;
	protected final Class<?> mapImplType;
	protected final Class<?> keyType;
	protected final Class<?> valueType;
	protected final boolean nullable;

	protected AbstractSerializerDefMap(@NotNull SerializerDef keySerializer, @NotNull SerializerDef valueSerializer, @NotNull Class<?> mapType, @NotNull Class<?> mapImplType, @NotNull Class<?> keyType, @NotNull Class<?> valueType, boolean nullable) {
		this.keySerializer = keySerializer;
		this.valueSerializer = valueSerializer;
		this.mapType = mapType;
		this.mapImplType = mapImplType;
		this.keyType = keyType;
		this.valueType = valueType;
		this.nullable = nullable;
	}

	protected abstract Expression mapForEach(Expression collection, Function<Expression, Expression> forEachKey, Function<Expression, Expression> forEachValue);

	protected Expression createConstructor(Expression length) {
		return constructor(mapImplType, !nullable ? length : dec(length));
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
	public Class<?> getRawType() {
		return mapType;
	}

	@Override
	public final Expression encoder(DefiningClassLoader classLoader, StaticEncoders staticEncoders, Expression buf, Variable pos, Expression value, int version, CompatibilityLevel compatibilityLevel) {
		return staticEncoders.define(mapType, buf, pos, value,
				serializeImpl(classLoader, staticEncoders, methodBuf(), methodPos(), methodValue(), version, compatibilityLevel));
	}

	private Expression serializeImpl(DefiningClassLoader classLoader, StaticEncoders staticEncoders, Expression buf, Variable pos, Expression value, int version, CompatibilityLevel compatibilityLevel) {
		Expression length = length(value);
		Expression writeLength = writeVarInt(buf, pos, !nullable ? length : inc(length));
		Expression forEach = mapForEach(value,
				k -> keySerializer.encoder(classLoader, staticEncoders, buf, pos, cast(k, keySerializer.getRawType()), version, compatibilityLevel),
				v -> valueSerializer.encoder(classLoader, staticEncoders, buf, pos, cast(v, valueSerializer.getRawType()), version, compatibilityLevel));

		return !nullable ?
				sequence(writeLength, forEach) :
				ifThenElse(isNull(value),
						writeByte(buf, pos, value((byte) 0)),
						sequence(writeLength, forEach));
	}

	@Override
	public final Expression decoder(DefiningClassLoader classLoader, StaticDecoders staticDecoders, Expression in, Class<?> targetType, int version, CompatibilityLevel compatibilityLevel) {
		checkArgument(targetType.isAssignableFrom(mapImplType), "Target(%s) should be assignable from map implementation type(%s)", targetType, mapImplType);
		return staticDecoders.define(targetType, in,
				deserializeImpl(classLoader, staticDecoders, methodIn(), version, compatibilityLevel));
	}

	private Expression deserializeImpl(DefiningClassLoader classLoader, StaticDecoders staticDecoders, Expression in, int version, CompatibilityLevel compatibilityLevel) {
		return let(readVarInt(in), length ->
				!nullable ?
						let(createConstructor(length), instance -> sequence(
								loop(value(0), length,
										it -> sequence(
												call(instance, "put",
														cast(keySerializer.decoder(classLoader, staticDecoders, in, keySerializer.getRawType(), version, compatibilityLevel), keyType),
														cast(valueSerializer.decoder(classLoader, staticDecoders, in, valueSerializer.getRawType(), version, compatibilityLevel), valueType)
												),
												voidExp())),
								instance)) :
						ifThenElse(
								cmpEq(length, value(0)),
								nullRef(mapImplType),
								let(createConstructor(length), instance -> sequence(
										loop(value(0), dec(length),
												it -> sequence(
														call(instance, "put",
																cast(keySerializer.decoder(classLoader, staticDecoders, in, keySerializer.getRawType(), version, compatibilityLevel), keyType),
																cast(valueSerializer.decoder(classLoader, staticDecoders, in, valueSerializer.getRawType(), version, compatibilityLevel), valueType)
														),
														voidExp())),
										instance))));
	}
}
