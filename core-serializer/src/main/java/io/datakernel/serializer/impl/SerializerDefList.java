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
import io.datakernel.codegen.Expressions;
import io.datakernel.codegen.Variable;
import io.datakernel.serializer.CompatibilityLevel;
import io.datakernel.serializer.SerializerDef;
import org.jetbrains.annotations.NotNull;

import java.util.Arrays;
import java.util.List;
import java.util.Set;

import static io.datakernel.codegen.Expressions.*;
import static io.datakernel.serializer.SerializerDef.StaticDecoders.IN;
import static io.datakernel.serializer.SerializerDef.StaticEncoders.*;
import static io.datakernel.serializer.impl.SerializerExpressions.*;
import static java.util.Collections.emptySet;

public final class SerializerDefList implements SerializerDefWithNullable {

	protected final SerializerDef valueSerializer;
	protected final Class<?> encodeType;
	protected final Class<?> decodeType;
	protected final Class<?> elementType;
	protected final boolean nullable;

	public SerializerDefList(SerializerDef valueSerializer) {
		this(valueSerializer, false);
	}

	private SerializerDefList(SerializerDef valueSerializer, boolean nullable) {
		this.valueSerializer = valueSerializer;
		this.encodeType = List.class;
		this.decodeType = List.class;
		this.elementType = Object.class;
		this.nullable = nullable;
	}

	@Override
	public SerializerDef ensureNullable() {
		return new SerializerDefList(valueSerializer, true);
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
	public Expression encoder(StaticEncoders staticEncoders, Expression buf, Variable pos, Expression list, int version, CompatibilityLevel compatibilityLevel) {
		if (!nullable) {
			return let(call(list, "size"),
					size -> sequence(
							writeVarInt(buf, pos, size),
							doEncode(staticEncoders, buf, pos, list, version, compatibilityLevel, size)));
		} else {
			return ifThenElse(isNull(list),
					writeByte(buf, pos, value((byte) 0)),
					let(call(list, "size"),
							size -> sequence(
									writeVarInt(buf, pos, inc(size)),
									doEncode(staticEncoders, buf, pos, list, version, compatibilityLevel, size))));
		}
	}

	@NotNull
	private Expression doEncode(StaticEncoders staticEncoders, Expression buf, Variable pos, Expression value, int version, CompatibilityLevel compatibilityLevel, Expression size) {
		return loop(value(0), size,
				i -> let(call(value, "get", i), item ->
						valueSerializer.defineEncoder(staticEncoders, buf, pos, cast(item, valueSerializer.getEncodeType()), version, compatibilityLevel)));
	}

	@Override
	public final Expression defineDecoder(StaticDecoders staticDecoders, Expression in, int version, CompatibilityLevel compatibilityLevel) {
		return staticDecoders.define(getDecodeType(), in,
				decoder(staticDecoders, IN, version, compatibilityLevel));
	}

	@Override
	public Expression decoder(StaticDecoders staticDecoders, Expression in, int version, CompatibilityLevel compatibilityLevel) {
		return let(readVarInt(in),
				size -> !nullable ?
						doDecode(staticDecoders, in, version, compatibilityLevel, size) :
						ifThenElse(cmpEq(size, value(0)),
								nullRef(decodeType),
								let(dec(size),
										size0 -> doDecode(staticDecoders, in, version, compatibilityLevel, size0))));
	}

	private Expression doDecode(StaticDecoders staticDecoders, Expression in, int version, CompatibilityLevel compatibilityLevel, Expression size) {
		return let(Expressions.newArray(Object[].class, size),
				array -> sequence(
						loop(value(0), size,
								i -> setArrayItem(array, i,
										cast(valueSerializer.defineDecoder(staticDecoders, in, version, compatibilityLevel), elementType))),
						callStatic(Arrays.class, "asList", array)));
	}
}
