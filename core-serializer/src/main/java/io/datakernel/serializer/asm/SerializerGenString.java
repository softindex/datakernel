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
import io.datakernel.serializer.BinaryOutputUtils;
import io.datakernel.serializer.CompatibilityLevel;
import io.datakernel.serializer.HasNullable;
import io.datakernel.serializer.StringFormat;

import java.util.Set;

import static io.datakernel.codegen.Expressions.*;
import static io.datakernel.common.Utils.of;
import static io.datakernel.serializer.StringFormat.UTF8;
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
	public Expression serialize(DefiningClassLoader classLoader, Expression buf, Variable pos, Expression value, int version, CompatibilityLevel compatibilityLevel) {
		return set(pos, of(() -> {
			Expression string = cast(value, String.class);
			switch (format) {
				case ISO_8859_1:
					return nullable ?
							callStatic(BinaryOutputUtils.class, "writeIso88591Nullable", buf, pos, string) :
							callStatic(BinaryOutputUtils.class, "writeIso88591", buf, pos, string);
				case UTF8:
					return nullable ?
							callStatic(BinaryOutputUtils.class, "writeUTF8Nullable", buf, pos, string) :
							callStatic(BinaryOutputUtils.class, "writeUTF8", buf, pos, string);
				case UTF16:
					return nullable ?
							callStatic(BinaryOutputUtils.class, "writeUTF16Nullable", buf, pos, string) :
							callStatic(BinaryOutputUtils.class, "writeUTF16", buf, pos, string);
				case UTF8_MB3:
					return nullable ?
							callStatic(BinaryOutputUtils.class, "writeUTF8mb3Nullable", buf, pos, string) :
							callStatic(BinaryOutputUtils.class, "writeUTF8mb3", buf, pos, string);
				default:
					throw new AssertionError();
			}
		}));
	}

	@Override
	public Expression deserialize(DefiningClassLoader classLoader, Expression in, Class<?> targetType, int version, CompatibilityLevel compatibilityLevel) {
		switch (format) {
			case ISO_8859_1:
				return nullable ?
						call(in, "readIso88591Nullable") :
						call(in, "readIso88591");
			case UTF8:
				return nullable ?
						call(in, "readUTF8Nullable") :
						call(in, "readUTF8");
			case UTF16:
				return nullable ?
						call(in, "readUTF16Nullable") :
						call(in, "readUTF16");
			case UTF8_MB3:
				return nullable ?
						call(in, "readUTF8mb3Nullable") :
						call(in, "readUTF8mb3");
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
