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
import io.datakernel.serializer.StringFormat;

import java.util.Set;

import static io.datakernel.codegen.Expressions.cast;
import static io.datakernel.serializer.CompatibilityLevel.LEVEL_4;
import static io.datakernel.serializer.StringFormat.UTF8;
import static io.datakernel.serializer.asm.SerializerExpressions.*;
import static java.util.Collections.emptySet;

public class SerializerGenString implements SerializerGen, HasNullable {
	private final StringFormat format;
	private final boolean nullable;

	private SerializerGenString(StringFormat format, boolean nullable) {
		this.format = format;
		this.nullable = nullable;
	}

	public SerializerGenString() {
		this(UTF8, false);
	}

	public SerializerGenString(StringFormat format) {
		this(format, false);
	}

	@Override
	public SerializerGenString withNullable() {
		return new SerializerGenString(format, true);
	}

	public SerializerGenString encoding(StringFormat format) {
		return new SerializerGenString(format, nullable);
	}

	@Override
	public void accept(Visitor visitor) {
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
		return String.class;
	}

	@Override
	public Expression serialize(DefiningClassLoader classLoader, Expression byteArray, Variable off, Expression value, int version, CompatibilityLevel compatibilityLevel) {
		Expression expression = cast(value, String.class);
		switch (format) {
			case UTF16:
				return nullable ?
						writeUTF16Nullable(byteArray, off, value, compatibilityLevel.compareTo(LEVEL_4) < 0) :
						writeUTF16(byteArray, off, value, compatibilityLevel.compareTo(LEVEL_4) < 0);
			case ISO_8859_1:
				return nullable ?
						writeIso88591Nullable(byteArray, off, expression) :
						writeIso88591(byteArray, off, expression);
			case UTF8:
				return nullable ?
						writeUTF8Nullable(byteArray, off, expression) :
						writeUTF8(byteArray, off, expression);
			case UTF8_MB3:
				return nullable ?
						writeUTF8mb3Nullable(byteArray, off, expression) :
						writeUTF8mb3(byteArray, off, expression);
			default:
				throw new AssertionError();
		}
	}

	@Override
	public Expression deserialize(DefiningClassLoader classLoader, Expression byteArray, Variable off, Class<?> targetType, int version, CompatibilityLevel compatibilityLevel) {
		switch (format) {
			case UTF16:
				return nullable ?
						readUTF16Nullable(byteArray, off, compatibilityLevel.compareTo(LEVEL_4) < 0) :
						readUTF16(byteArray, off, compatibilityLevel.compareTo(LEVEL_4) < 0);
			case ISO_8859_1:
				return nullable ?
						readIso88591Nullable(byteArray, off) :
						readIso88591(byteArray, off);
			case UTF8:
				return nullable ?
						readUTF8Nullable(byteArray, off) :
						readUTF8(byteArray, off);
			case UTF8_MB3:
				return nullable ?
						readUTF8mb3Nullable(byteArray, off) :
						readUTF8mb3(byteArray, off);
			default:
				throw new AssertionError();
		}
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) return true;
		if (o == null || getClass() != o.getClass()) return false;

		SerializerGenString that = (SerializerGenString) o;

		if (nullable != that.nullable) return false;
		return format == that.format;
	}

	@Override
	public int hashCode() {
		int result = format != null ? format.hashCode() : 0;
		result = 31 * result + (nullable ? 1 : 0);
		return result;
	}
}
