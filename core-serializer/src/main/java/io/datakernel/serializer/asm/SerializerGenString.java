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
import io.datakernel.serializer.StringFormat;
import io.datakernel.serializer.util.BinaryOutputUtils;

import java.util.Set;

import static io.datakernel.codegen.Expressions.*;
import static io.datakernel.serializer.StringFormat.UTF8;
import static io.datakernel.serializer.asm.SerializerExpressions.*;
import static java.util.Collections.emptySet;

public class SerializerGenString implements SerializerGen {
	private final StringFormat format;
	private final boolean nullable;

	public SerializerGenString(boolean nullable, StringFormat format) {
		this.format = format;
		this.nullable = nullable;
	}

	public SerializerGenString() {
		this(false, UTF8);
	}

	public SerializerGenString(StringFormat format) {
		this(false, format);
	}

	public SerializerGenString nullable(boolean nullable) {
		return new SerializerGenString(nullable, format);
	}

	public SerializerGenString encoding(StringFormat format) {
		return new SerializerGenString(nullable, format);
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
						set(off, callStatic(BinaryOutputUtils.class, "writeUTF16Nullable", byteArray, off, expression)) :
						set(off, callStatic(BinaryOutputUtils.class, "writeUTF16", byteArray, off, expression));
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
						set(off, callStatic(BinaryOutputUtils.class, "writeUTF8mb3Nullable", byteArray, off, expression)) :
						set(off, callStatic(BinaryOutputUtils.class, "writeUTF8mb3", byteArray, off, expression));
			default:
				throw new AssertionError();
		}
	}

	@Override
	public Expression deserialize(DefiningClassLoader classLoader, Expression byteArray, Variable off, Class<?> targetType, int version, CompatibilityLevel compatibilityLevel) {
		switch (format) {
			case UTF16:
				return nullable ?
						call(arg(0), "readUTF16Nullable") :
						call(arg(0), "readUTF16");
			case ISO_8859_1:
				return nullable ?
						call(arg(0), "readIso88591Nullable") :
						call(arg(0), "readIso88591");
			case UTF8:
				return nullable ?
						call(arg(0), "readUTF8Nullable") :
						call(arg(0), "readUTF8");
			case UTF8_MB3:
				return nullable ?
						call(arg(0), "readUTF8mb3Nullable") :
						call(arg(0), "readUTF8mb3");
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
