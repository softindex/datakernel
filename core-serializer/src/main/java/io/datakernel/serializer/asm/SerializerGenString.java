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
import io.datakernel.codegen.Variable;
import io.datakernel.serializer.CompatibilityLevel;
import io.datakernel.serializer.SerializerBuilder.StaticMethods;
import io.datakernel.serializer.StringFormat;
import io.datakernel.serializer.util.BinaryOutputUtils;

import java.util.ArrayList;
import java.util.List;

import static io.datakernel.codegen.Expressions.*;

public class SerializerGenString implements SerializerGen {
	private final StringFormat format;
	private final boolean nullable;

	public SerializerGenString(boolean nullable, StringFormat format) {
		this.format = format;
		this.nullable = nullable;
	}

	public SerializerGenString() {
		this(false, StringFormat.UTF8);
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
	public void prepareSerializeStaticMethods(int version, StaticMethods staticMethods, CompatibilityLevel compatibilityLevel) {

	}

	@SuppressWarnings("deprecation") // compatibility
	@Override
	public Expression serialize(Expression byteArray, Variable off, Expression value, int version, StaticMethods staticMethods, CompatibilityLevel compatibilityLevel) {
		List<Expression> list = new ArrayList<>();

		Expression expression = cast(value, String.class);

		if (format == StringFormat.UTF16) {
			if (nullable)
				list.add(callStatic(BinaryOutputUtils.class, "writeUTF16Nullable", byteArray, off, expression));
			else
				list.add(callStatic(BinaryOutputUtils.class, "writeUTF16", byteArray, off, expression));
		} else if (format == StringFormat.ISO_8859_1 && compatibilityLevel != CompatibilityLevel.LEVEL_1) {
			if (nullable)
				list.add(callStatic(BinaryOutputUtils.class, "writeIso88591Nullable", byteArray, off, expression));
			else
				list.add(callStatic(BinaryOutputUtils.class, "writeIso88591", byteArray, off, expression));
		} else if (format == StringFormat.UTF8 && compatibilityLevel != CompatibilityLevel.LEVEL_1) {
			if (nullable)
				list.add(callStatic(BinaryOutputUtils.class, "writeUTF8Nullable", byteArray, off, expression));
			else
				list.add(callStatic(BinaryOutputUtils.class, "writeUTF8", byteArray, off, expression));
		} else {
			if (nullable)
				list.add(callStatic(BinaryOutputUtils.class, "writeUTF8mb3Nullable", byteArray, off, expression));
			else
				list.add(callStatic(BinaryOutputUtils.class, "writeUTF8mb3", byteArray, off, expression));
		}

		return sequence(list);

	}

	@Override
	public void prepareDeserializeStaticMethods(int version, StaticMethods staticMethods, CompatibilityLevel compatibilityLevel) {

	}

	@SuppressWarnings("deprecation") // compatibility
	@Override
	public Expression deserialize(Class<?> targetType, int version, StaticMethods staticMethods, CompatibilityLevel compatibilityLevel) {
		if (format == StringFormat.UTF16) {
			if (nullable)
				return call(arg(0), "readUTF16Nullable");
			else
				return call(arg(0), "readUTF16");
		} else if (format == StringFormat.ISO_8859_1 && compatibilityLevel != CompatibilityLevel.LEVEL_1) {
			if (nullable)
				return call(arg(0), "readIso88591Nullable");
			else
				return call(arg(0), "readIso88591");
		} else if (format == StringFormat.UTF8 && compatibilityLevel != CompatibilityLevel.LEVEL_1) {
			if (nullable)
				return call(arg(0), "readUTF8Nullable");
			else
				return call(arg(0), "readUTF8");
		} else {
			if (nullable)
				return call(arg(0), "readUTF8mb3Nullable");
			else
				return call(arg(0), "readUTF8mb3");
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
