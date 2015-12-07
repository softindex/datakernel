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
import io.datakernel.serializer.SerializerBuilder;

import java.util.Collection;
import java.util.HashSet;
import java.util.Set;

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
			this.versions.add(version);
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

	Class<?> getRawType();

	void prepareSerializeStaticMethods(int version, SerializerBuilder.StaticMethods staticMethods, CompatibilityLevel compatibilityLevel);

	Expression serialize(Expression byteArray, Variable off, Expression value, int version, SerializerBuilder.StaticMethods staticMethods, CompatibilityLevel compatibilityLevel);

	void prepareDeserializeStaticMethods(int version, SerializerBuilder.StaticMethods staticMethods, CompatibilityLevel compatibilityLevel);

	Expression deserialize(Class<?> targetType, int version, SerializerBuilder.StaticMethods staticMethods, CompatibilityLevel compatibilityLevel);

}
