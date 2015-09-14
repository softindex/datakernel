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

package io.datakernel.jmx;

import javax.management.openmbean.*;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

import static io.datakernel.util.Preconditions.checkNotNull;

public final class CompositeDataBuilder {
	public static class Builder {
		private final String typeName;
		private final String description;
		private final Map<String, OpenType<?>> types = new HashMap<>();
		private final Map<String, String> descriptions = new HashMap<>();
		private final Map<String, Object> values = new HashMap<>();

		public Builder(String typeName, String description) {
			this.typeName = typeName;
			this.description = description;
		}

		public Builder add(String name, CompositeData data) throws OpenDataException {
			if (data != null)
				return add(name, data.getCompositeType(), data);
			return add(name, SimpleType.VOID, null);
		}

		public Builder add(String name, OpenType<?> type, Object value) throws OpenDataException {
			return add(name, name, type, value);
		}

		public Builder add(String name, String description, OpenType<?> type, Object value) throws OpenDataException {
			checkNotNull(name);
			checkNotNull(type);
			if (value != null && !type.isValue(value))
				throw new OpenDataException("Argument value of wrong type for item " + name + ": value " + value + ", type " + type);
			if (types.containsKey(name)) {
				throw new IllegalArgumentException("Argument type already exist for item " + name);
			}
			types.put(name, type);
			values.put(name, value);
			descriptions.put(name, description);
			return this;
		}

		public CompositeData build() throws OpenDataException {
			String[] itemNames = new String[types.size()];
			String[] itemDescriptions = new String[types.size()];
			OpenType<?>[] itemTypes = new OpenType<?>[types.size()];
			int i = 0;
			for (Entry<String, OpenType<?>> entry : types.entrySet()) {
				itemNames[i] = entry.getKey();
				itemTypes[i] = entry.getValue();
				itemDescriptions[i] = descriptions.get(entry.getKey());
				++i;
			}
			CompositeType compositeType = new CompositeType(typeName, description, itemNames, itemDescriptions, itemTypes);
			return new CompositeDataSupport(compositeType, values);
		}
	}

	public static Builder builder(String typeName) {
		return builder(typeName, typeName);
	}

	public static Builder builder(String typeName, String description) {
		return new Builder(typeName, description);
	}

	private CompositeDataBuilder() {
	}
}
