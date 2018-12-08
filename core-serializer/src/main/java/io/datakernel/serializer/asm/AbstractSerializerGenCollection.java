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

import io.datakernel.codegen.Expression;
import io.datakernel.codegen.ExpressionParameter;
import io.datakernel.codegen.Variable;
import io.datakernel.serializer.CompatibilityLevel;
import io.datakernel.serializer.NullableOptimization;
import io.datakernel.serializer.SerializerBuilder.StaticMethods;
import io.datakernel.serializer.util.BinaryOutputUtils;

import java.util.function.Function;

import static io.datakernel.codegen.Expressions.*;
import static io.datakernel.util.Preconditions.check;
import static io.datakernel.util.Preconditions.checkNotNull;

public abstract class AbstractSerializerGenCollection implements SerializerGen, NullableOptimization {
	protected final SerializerGen valueSerializer;
	protected final Class<?> collectionType;
	protected final Class<?> collectionImplType;
	protected final Class<?> elementType;
	protected final boolean nullable;

	protected AbstractSerializerGenCollection(SerializerGen valueSerializer, Class<?> collectionType, Class<?> collectionImplType, Class<?> elementType, boolean nullable) {
		this.valueSerializer = checkNotNull(valueSerializer);
		this.collectionType = checkNotNull(collectionType);
		this.collectionImplType = checkNotNull(collectionImplType);
		this.elementType = checkNotNull(elementType);
		this.nullable = nullable;
	}

	protected Expression collectionForEach(Expression collection, Class<?> valueType, Function<ExpressionParameter, Expression> value) {
		return forEach(collection, valueType, value);
	}

	protected Expression createConstructor(Expression length) {
		return let(constructor(collectionImplType, (!nullable ? length : dec(length))));
	}

	@Override
	public void getVersions(VersionsCollector versions) {
		versions.addRecursive(valueSerializer);
	}

	@Override
	public boolean isInline() {
		return true;
	}

	@Override
	public Class<?> getRawType() {
		return collectionType;
	}

	@Override
	public void prepareSerializeStaticMethods(int version, StaticMethods staticMethods, CompatibilityLevel compatibilityLevel) {
		valueSerializer.prepareSerializeStaticMethods(version, staticMethods, compatibilityLevel);
	}

	@Override
	public final Expression serialize(Expression byteArray, Variable off, Expression value, int version, StaticMethods staticMethods, CompatibilityLevel compatibilityLevel) {
		Expression forEach = collectionForEach(value, valueSerializer.getRawType(),
				it -> set(off, valueSerializer.serialize(byteArray, off, cast(it, valueSerializer.getRawType()), version, staticMethods, compatibilityLevel)));

		if (nullable) {
			return ifThenElse(isNull(value),
					sequence(set(off, callStatic(BinaryOutputUtils.class, "writeVarInt", byteArray, off, value(0))), off),
					sequence(set(off, callStatic(BinaryOutputUtils.class, "writeVarInt", byteArray, off, inc(call(value, "size")))), forEach, off));
		}
		return sequence(
				set(off, callStatic(BinaryOutputUtils.class, "writeVarInt", byteArray, off, call(value, "size"))), forEach, off);
	}

	@Override
	public final Expression deserialize(Class<?> targetType, int version, StaticMethods staticMethods, CompatibilityLevel compatibilityLevel) {
		check(targetType.isAssignableFrom(collectionImplType), "Target(%s) should be assignable from collection implementation type(%s)", targetType, collectionImplType);
		Expression length = let(call(arg(0), "readVarInt"));
		Expression container = createConstructor(length);

		Expression deserializeEach = expressionFor(value(0), !nullable ? length : dec(length),
				it -> sequence(
						call(container, "add", cast(valueSerializer.deserialize(elementType, version, staticMethods, compatibilityLevel), elementType)),
						voidExp()));
		if (!nullable) {
			return sequence(container, deserializeEach, container);
		}
		return ifThenElse(cmpEq(length, value(0)),
				nullRef(collectionImplType),
				sequence(container, deserializeEach, container));
	}

	@Override
	public void prepareDeserializeStaticMethods(int version, StaticMethods staticMethods, CompatibilityLevel compatibilityLevel) {
		valueSerializer.prepareDeserializeStaticMethods(version, staticMethods, compatibilityLevel);
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) return true;
		if (!(o instanceof AbstractSerializerGenCollection)) return false;

		AbstractSerializerGenCollection that = (AbstractSerializerGenCollection) o;

		if (nullable != that.nullable) return false;
		if (!valueSerializer.equals(that.valueSerializer)) return false;
		if (!collectionType.equals(that.collectionType)) return false;
		if (!collectionImplType.equals(that.collectionImplType)) return false;
		return elementType.equals(that.elementType);
	}

	@Override
	public int hashCode() {
		int result = valueSerializer.hashCode();
		result = 31 * result + collectionType.hashCode();
		result = 31 * result + collectionImplType.hashCode();
		result = 31 * result + elementType.hashCode();
		result = 31 * result + (nullable ? 1 : 0);
		return result;
	}
}
