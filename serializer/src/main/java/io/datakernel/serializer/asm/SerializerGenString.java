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

package io.datakernel.serializer.asm;

import io.datakernel.codegen.Expression;
import io.datakernel.codegen.Variable;
import io.datakernel.codegen.utils.Preconditions;
import io.datakernel.serializer.CompatibilityLevel;
import io.datakernel.serializer.SerializerBuilder;
import io.datakernel.serializer.SerializerUtils;
import io.datakernel.serializer.StringFormat;

import java.util.ArrayList;
import java.util.List;

import static io.datakernel.codegen.Expressions.*;

public class SerializerGenString implements SerializerGen {
	private final StringFormat format;
	private final boolean nullable;
	private final int maxLength;

	public SerializerGenString(int maxLength, boolean nullable, StringFormat format) {
		Preconditions.check(maxLength == -1 || maxLength > 0);
		this.maxLength = maxLength;
		this.format = format;
		this.nullable = nullable;
	}

	public SerializerGenString() {
		this(-1, false, StringFormat.UTF8);
	}

	public SerializerGenString(StringFormat format) {
		this(-1, false, format);
	}

	public SerializerGenString nullable(boolean nullable) {
		return new SerializerGenString(maxLength, nullable, format);
	}

	public SerializerGenString encoding(StringFormat format) {
		return new SerializerGenString(maxLength, nullable, format);
	}

	public SerializerGen maxLength(int maxLength) {
		return new SerializerGenString(maxLength, nullable, format);
	}

	@Override
	public void getVersions(VersionsCollector versions) {
	}

	@Override
	public boolean isInline() {
		return true;
	}

	@Override
	public Class<?> getRawType() {
		return String.class;
	}

	@Override
	public void prepareSerializeStaticMethods(int version, SerializerBuilder.StaticMethods staticMethods, CompatibilityLevel compatibilityLevel) {

	}

	@Override
	public Expression serialize(Expression byteArray, Variable off, Expression value, int version, SerializerBuilder.StaticMethods staticMethods, CompatibilityLevel compatibilityLevel) {
		List<Expression> list = new ArrayList<>();

		Expression maxLen = value(maxLength);
		Expression expression;
		if (maxLength != -1) {
			expression = choice(and(isNotNull(value), cmpGe(maxLen, value(0)), cmpGe(call(cast(value, String.class), "length"), value(maxLength + 1))),
					cast(call(cast(value, String.class), "substring", value(0), maxLen), String.class),
					cast(value, String.class));
		} else {
			expression = cast(value, String.class);
		}

		if (format == StringFormat.UTF16) {
			if (nullable)
				list.add(callStatic(SerializerUtils.class, "writeNullableUTF16", byteArray, off, expression));
			else
				list.add(callStatic(SerializerUtils.class, "writeUTF16", byteArray, off, expression));
		} else if (format == StringFormat.ISO_8859_1 && compatibilityLevel != CompatibilityLevel.LEVEL_1) {
			if (nullable)
				list.add(callStatic(SerializerUtils.class, "writeNullableIso88591", byteArray, off, expression));
			else
				list.add(callStatic(SerializerUtils.class, "writeIso88591", byteArray, off, expression));
		} else if (format == StringFormat.UTF8 && compatibilityLevel != CompatibilityLevel.LEVEL_1) {
			if (nullable)
				list.add(callStatic(SerializerUtils.class, "writeNullableJavaUTF8", byteArray, off, expression));
			else
				list.add(callStatic(SerializerUtils.class, "writeJavaUTF8", byteArray, off, expression));
		} else {
			if (nullable)
				list.add(callStatic(SerializerUtils.class, "writeNullableUTF8", byteArray, off, expression));
			else
				list.add(callStatic(SerializerUtils.class, "writeUTF8", byteArray, off, expression));
		}

		return sequence(list);

	}

	@Override
	public void prepareDeserializeStaticMethods(int version, SerializerBuilder.StaticMethods staticMethods, CompatibilityLevel compatibilityLevel) {

	}

	@Override
	public Expression deserialize(Class<?> targetType, int version, SerializerBuilder.StaticMethods staticMethods, CompatibilityLevel compatibilityLevel) {
		if (format == StringFormat.UTF16) {
			if (nullable)
				return call(arg(0), "readNullableUTF16");
			else
				return call(arg(0), "readUTF16");
		} else if (format == StringFormat.ISO_8859_1 && compatibilityLevel != CompatibilityLevel.LEVEL_1) {
			if (nullable)
				return call(arg(0), "readNullableIso88591");
			else
				return call(arg(0), "readIso88591");
		} else if (format == StringFormat.UTF8 && compatibilityLevel != CompatibilityLevel.LEVEL_1) {
			if (nullable)
				return call(arg(0), "readNullableJavaUTF8");
			else
				return call(arg(0), "readJavaUTF8");
		} else {
			if (nullable)
				return call(arg(0), "readNullableUTF8");
			else
				return call(arg(0), "readUTF8");
		}
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) return true;
		if (o == null || getClass() != o.getClass()) return false;

		SerializerGenString that = (SerializerGenString) o;

		if (nullable != that.nullable) return false;
		if (maxLength != that.maxLength) return false;
		return format == that.format;

	}

	@Override
	public int hashCode() {
		int result = format != null ? format.hashCode() : 0;
		result = 31 * result + (nullable ? 1 : 0);
		result = 31 * result + maxLength;
		return result;
	}
}
