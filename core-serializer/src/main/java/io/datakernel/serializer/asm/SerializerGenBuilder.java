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

import org.jetbrains.annotations.NotNull;

import static io.datakernel.common.Preconditions.checkArgument;

@FunctionalInterface
public interface SerializerGenBuilder {

	final class SerializerForType {
		public final Class<?> rawType;
		public final SerializerGen serializer;

		public SerializerForType(@NotNull Class<?> rawType, @NotNull SerializerGen serializer) {
			this.rawType = rawType;
			this.serializer = serializer;
		}

		@SuppressWarnings("RedundantIfStatement")
		@Override
		public boolean equals(Object o) {
			if (this == o) return true;
			if (o == null || getClass() != o.getClass()) return false;

			SerializerForType that = (SerializerForType) o;

			if (!rawType.equals(that.rawType)) return false;
			if (!serializer.equals(that.serializer)) return false;

			return true;
		}

		@Override
		public int hashCode() {
			int result = rawType.hashCode();
			result = 31 * result + serializer.hashCode();
			return result;
		}

		@Override
		public String toString() {
			return rawType.getName();
		}
	}

	SerializerGen serializer(Class<?> type, SerializerForType[] generics, SerializerGen target);

	static SerializerGenBuilder of(SerializerGen serializer) {
		return (type, generics, target) -> {
			checkArgument(generics.length == 0, "Type should have no generics");
			return serializer;
		};
	}
}
