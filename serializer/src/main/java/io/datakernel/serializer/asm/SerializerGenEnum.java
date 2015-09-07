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
import io.datakernel.serializer.SerializerBuilder;
import org.objectweb.asm.Type;

import static io.datakernel.codegen.Expressions.*;

@SuppressWarnings("StatementWithEmptyBody")
public class SerializerGenEnum implements SerializerGen {
	private Class<?> nameOfEnum;

	public SerializerGenEnum(Class<?> c) {
		nameOfEnum = c;
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
		return nameOfEnum;
	}

	@Override
	public void prepareSerializeStaticMethods(int version, SerializerBuilder.StaticMethods staticMethods) {

	}

	@Override
	public Expression serialize(Expression value, int version, SerializerBuilder.StaticMethods staticMethods) {
		return call(arg(0), "writeByte", cast(call(cast(value, Enum.class), "ordinal"), Type.BYTE_TYPE));
	}

	@Override
	public void prepareDeserializeStaticMethods(int version, SerializerBuilder.StaticMethods staticMethods) {

	}

	@Override
	public Expression deserialize(Class<?> targetType, int version, SerializerBuilder.StaticMethods staticMethods) {
		return get(callStatic(nameOfEnum, "values"), call(arg(0), "readByte"));
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) return true;
		if (o == null || getClass() != o.getClass()) return false;

//        SerializerGenEnum that = (SerializerGenEnum) o;

		return true;
	}

	@Override
	public int hashCode() {
		return 0;
	}

}
