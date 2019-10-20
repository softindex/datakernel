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

/**
 * Represents a serializer and deserializer of a particular class to byte arrays
 */
public interface SerializerGen {

	interface Visitor {
		void visit(String subcomponentId, SerializerGen site);

		default void visit(SerializerGen site) {
			visit("", site);
		}
	}

	void accept(Visitor visitor);

	Set<Integer> getVersions();

	boolean isInline();

	/**
	 * Returns the raw type of object which will be serialized
	 *
	 * @return type of object which will be serialized
	 */
	Class<?> getRawType();

	/**
	 * Serializes provided {@link Expression} {@code value} to byte array
	 *
	 * @param buf                byte array to which the value will be serialized
	 * @param pos                an offset in the byte array
	 * @param value              the value to be serialized to byte array
	 * @param compatibilityLevel defines the {@link CompatibilityLevel compatibility level} of the serializer
	 * @return serialized to byte array value
	 */
	Expression serialize(DefiningClassLoader classLoader, Expression buf, Variable pos, Expression value, int version, CompatibilityLevel compatibilityLevel);

	/**
	 * Deserializes object of {@code targetType} from byte array
	 *
	 * @param in                 BinaryInput
	 * @param targetType         target type which is used to deserialize byte array
	 * @param compatibilityLevel defines the {@link CompatibilityLevel compatibility level} of the serializer
	 * @return deserialized {@code Expression} object of provided targetType
	 */
	Expression deserialize(DefiningClassLoader classLoader, Expression in, Class<?> targetType, int version, CompatibilityLevel compatibilityLevel);

	@Override
	boolean equals(Object o);

	@Override
	int hashCode();
}
