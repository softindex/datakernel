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
import io.datakernel.serializer.CompatibilityLevel;
import io.datakernel.serializer.SerializerBuilder.StaticMethods;

import java.util.Collection;
import java.util.HashSet;
import java.util.Set;

/**
 * Represents a serializer and deserializer of a particular class to byte arrays
 */
public interface SerializerGen {

	final class VersionsCollector {
		private final Set<Integer> versions = new HashSet<>();
		private final Set<SerializerGen> collected = new HashSet<>();

		public static Set<Integer> versions(SerializerGen serializerGen) {
			VersionsCollector versions = new VersionsCollector();
			versions.collected.add(serializerGen);
			serializerGen.getVersions(versions);
			return versions.versions;
		}

		public void add(int version) {
			versions.add(version);
		}

		public void addAll(Collection<Integer> versions) {
			this.versions.addAll(versions);
		}

		public void addRecursive(SerializerGen serializerGen) {
			if (!collected.contains(serializerGen)) {
				collected.add(serializerGen);
				serializerGen.getVersions(this);
			}
		}
	}

	void getVersions(VersionsCollector versions);

	boolean isInline();

	/**
	 * Returns the raw type of object which will be serialized
	 *
	 * @return type of object which will be serialized
	 */
	Class<?> getRawType();

	void prepareSerializeStaticMethods(int version, StaticMethods staticMethods, CompatibilityLevel compatibilityLevel);

	/**
	 * Serializes provided {@link Expression} {@code value} to byte array
	 *
	 * @param byteArray byte array to which the value will be serialized
	 * @param off an offset in the byte array
	 * @param value the value to be serialized to byte array
	 * @param compatibilityLevel defines the {@link CompatibilityLevel compatibility level} of the serializer
	 * @return serialized to byte array value
	 */
	Expression serialize(Expression byteArray, Variable off, Expression value, int version, StaticMethods staticMethods, CompatibilityLevel compatibilityLevel);

	void prepareDeserializeStaticMethods(int version, StaticMethods staticMethods, CompatibilityLevel compatibilityLevel);

	/**
	 * Deserializes object of {@code targetType} from byte array
	 *
	 * @param targetType target type which is used to deserialize byte array
	 * @param compatibilityLevel defines the {@link CompatibilityLevel compatibility level} of the serializer
	 * @return deserialized {@code Expression} object of provided targetType
	 */
	Expression deserialize(Class<?> targetType, int version, StaticMethods staticMethods, CompatibilityLevel compatibilityLevel);

}
