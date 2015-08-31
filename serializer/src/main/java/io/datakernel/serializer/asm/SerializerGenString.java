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

import io.datakernel.codegen.FunctionDef;
import io.datakernel.codegen.utils.Preconditions;
import io.datakernel.serializer.SerializerFactory;

import java.util.ArrayList;
import java.util.List;

import static io.datakernel.codegen.FunctionDefs.*;

public class SerializerGenString implements SerializerGen {
	private final boolean utf16;
	private final boolean nullable;
	private final boolean ascii;
	private final int maxLength;

	public SerializerGenString(boolean utf16, boolean nullable, int maxLength, boolean ascii) {
		Preconditions.check(maxLength == -1 || maxLength > 0);
		this.utf16 = utf16;
		this.nullable = nullable;
		this.maxLength = maxLength;
		this.ascii = ascii;
	}

	public SerializerGenString(boolean utf16, boolean nullable, int maxLength) {
		Preconditions.check(maxLength == -1 || maxLength > 0);
		this.utf16 = utf16;
		this.nullable = nullable;
		this.maxLength = maxLength;
		this.ascii = false;
	}

	public SerializerGenString() {
		this(false, false, -1, false);
	}

	public SerializerGenString(boolean utf16, boolean nullable) {
		this(utf16, nullable, -1, false);
	}

	public SerializerGenString ascii(boolean ascii) {
		return new SerializerGenString(false, nullable, maxLength, ascii);
	}

	public SerializerGenString utf16(boolean utf16) {
		return new SerializerGenString(utf16, nullable, maxLength, ascii);
	}

	public SerializerGenString nullable(boolean nullable) {
		return new SerializerGenString(utf16, nullable, maxLength, ascii);
	}

	public SerializerGen maxLength(int maxLength) {
		return new SerializerGenString(utf16, nullable, maxLength, ascii);
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
	public void prepareSerializeStaticMethods(int version, SerializerFactory.StaticMethods staticMethods) {

	}

	@Override
	public FunctionDef serialize(FunctionDef value, int version, SerializerFactory.StaticMethods staticMethods) {
		List<FunctionDef> list = new ArrayList<>();

		FunctionDef maxLen = value(maxLength);
		FunctionDef function;
		if (maxLength != -1) {
			function = choice(and(cmpGe(maxLen, value(0)), cmpGe(call(cast(value, String.class), "length"), value(maxLength + 1))),
					cast(call(cast(value, String.class), "substring", value(0), maxLen), String.class),
					cast(value, String.class));
		} else {
			function = cast(value, String.class);
		}

		if (utf16) {
			if (nullable)
				list.add(call(arg(0), "writeNullableUTF16", function));
			else
				list.add(call(arg(0), "writeUTF16", function));
		} else if (ascii) {
			if (nullable)
				list.add(call(arg(0), "writeNullableAscii", function));
			else
				list.add(call(arg(0), "writeAscii", function));
		} else {
			if (nullable)
				list.add(call(arg(0), "writeNullableUTF8", function));
			else
				list.add(call(arg(0), "writeUTF8", function));
		}
		return sequence(list);
	}

	@Override
	public void prepareDeserializeStaticMethods(int version, SerializerFactory.StaticMethods staticMethods) {

	}

	@Override
	public FunctionDef deserialize(Class<?> targetType, int version, SerializerFactory.StaticMethods staticMethods) {
		if (utf16) {
			if (nullable)
				return call(arg(0), "readNullableUTF16");
			else
				return call(arg(0), "readUTF16");
		} else if (ascii) {
			if (nullable)
				return call(arg(0), "readNullableAscii");
			else
				return call(arg(0), "readAscii");
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
		if (utf16 != that.utf16) return false;

		return true;
	}

	@Override
	public int hashCode() {
		int result = (utf16 ? 1 : 0);
		result = 31 * result + (nullable ? 1 : 0);
		return result;
	}
}
