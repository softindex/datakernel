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

import com.google.common.base.Objects;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import java.util.List;
import java.util.Map;

import static io.datakernel.serializer.asm.SerializerGenBuilder.SerializerForType;

public final class TypedModsMap {
	private static final TypedModsMap EMPTY = new TypedModsMap();

	private final ImmutableList<SerializerGenBuilder> mods;
	private final ImmutableMap<Integer, TypedModsMap> children;

	public static class Builder {
		private List<SerializerGenBuilder> mods = Lists.newArrayList();
		private Map<Integer, Builder> children = Maps.newLinkedHashMap();

		public void add(SerializerGenBuilder serializerGenBuilder) {
			this.mods.add(serializerGenBuilder);
		}

		private Builder ensureChild(int childKey) {
			Builder result = children.get(childKey);
			if (result == null) {
				result = new Builder();
				children.put(childKey, result);
			}
			return result;
		}

		public Builder ensureChild(int[] path) {
			Builder result = this;
			for (int i = 0; i < path.length; i++) {
				int n = path[i];
				result = result.ensureChild(n);
			}
			return result;
		}

		public TypedModsMap build() {
			if (this.mods.isEmpty() && this.children.isEmpty())
				return empty();
			return new TypedModsMap(this);
		}
	}

	public static Builder builder() {
		return new Builder();
	}

	public static TypedModsMap empty() {
		return EMPTY;
	}

	private TypedModsMap() {
		this.mods = ImmutableList.of();
		this.children = ImmutableMap.of();
	}

	private TypedModsMap(Builder builder) {
		this.mods = ImmutableList.copyOf(builder.mods);
		ImmutableMap.Builder<Integer, TypedModsMap> childrenBuilder = ImmutableMap.builder();
		for (Integer key : builder.children.keySet()) {
			childrenBuilder.put(key, new TypedModsMap(builder.children.get(key)));
		}
		this.children = childrenBuilder.build();
	}

	public boolean hasChildren() {
		return !children.isEmpty();
	}

	public ImmutableList<SerializerGenBuilder> getMods() {
		return mods;
	}

	public boolean isEmpty() {
		return children.isEmpty() && mods.isEmpty();
	}

	public TypedModsMap get(int typeIndex) {
		TypedModsMap result = children.get(typeIndex);
		return result == null ? empty() : result;
	}

	public SerializerGen rewrite(final Class<?> type, final SerializerForType[] generics, final SerializerGen serializer) {
		SerializerGen result = serializer;
		for (SerializerGenBuilder mod : mods) {
			result = mod.serializer(type, generics, result);
		}
		return result;
	}

	@Override
	public String toString() {
		return Objects.toStringHelper(this)
				.add("mods", mods)
				.add("children", children)
				.toString();
	}

}
