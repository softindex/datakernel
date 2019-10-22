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

import io.datakernel.codegen.DefiningClassLoader;
import io.datakernel.codegen.Expression;
import io.datakernel.codegen.Variable;
import io.datakernel.serializer.CompatibilityLevel;

import java.util.Set;

import static io.datakernel.codegen.Expressions.arg;

/**
 * Represents a serializer and deserializer of a particular class to byte arrays
 */
public interface SerializerDef {

	interface Visitor {
		void visit(String serializerId, SerializerDef serializer);

		default void visit(SerializerDef serializer) {
			visit("", serializer);
		}
	}

	void accept(Visitor visitor);

	Set<Integer> getVersions();

	/**
	 * Returns the raw type of object which will be serialized
	 *
	 * @return type of object which will be serialized
	 */
	Class<?> getRawType();

	interface StaticEncoders {
		static Expression methodBuf() { return arg(0);}

		static Variable methodPos() { return arg(1);}

		static Expression methodValue() { return arg(2);}

		Expression define(Class<?> valueClazz, Expression buf, Variable pos, Expression value, Expression method);
	}

	/**
	 * Serializes provided {@link Expression} {@code value} to byte array
	 *
	 * @param staticEncoders
	 * @param buf                byte array to which the value will be serialized
	 * @param pos                an offset in the byte array
	 * @param value              the value to be serialized to byte array
	 * @param compatibilityLevel defines the {@link CompatibilityLevel compatibility level} of the serializer
	 * @return serialized to byte array value
	 */
	Expression encoder(DefiningClassLoader classLoader, StaticEncoders staticEncoders, Expression buf, Variable pos, Expression value, int version, CompatibilityLevel compatibilityLevel);

	interface StaticDecoders {
		static Expression methodIn() { return arg(0);}

		Expression define(Class<?> valueClazz, Expression in, Expression method);
	}

	/**
	 * Deserializes object of {@code targetType} from byte array
	 *
	 *
	 * @param staticDecoders
	 * @param in                 BinaryInput
	 * @param targetType         target type which is used to deserialize byte array
	 * @param compatibilityLevel defines the {@link CompatibilityLevel compatibility level} of the serializer
	 * @return deserialized {@code Expression} object of provided targetType
	 */
	Expression decoder(DefiningClassLoader classLoader, StaticDecoders staticDecoders, Expression in, Class<?> targetType, int version, CompatibilityLevel compatibilityLevel);

	@Override
	boolean equals(Object o);

	@Override
	int hashCode();
}
