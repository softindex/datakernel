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
import java.lang.reflect.Array;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static io.datakernel.jmx.OpenTypeUtils.classOf;
import static io.datakernel.jmx.OpenTypeUtils.createMapWithOneEntry;
import static io.datakernel.jmx.Utils.filterNulls;
import static io.datakernel.util.Preconditions.checkArgument;
import static io.datakernel.util.Preconditions.checkNotNull;
import static java.util.Arrays.asList;

final class AttributeNodeForList implements AttributeNode {
	private final String name;
	private final ValueFetcher fetcher;
	private final AttributeNode subNode;
	private final ArrayType<?> arrayType;
	private final Map<String, OpenType<?>> nameToOpenType;
	private final boolean refreshable;

	public AttributeNodeForList(String name, ValueFetcher fetcher, AttributeNode subNode) {
		checkArgument(!name.isEmpty(), "List attribute cannot have empty name");

		this.name = name;
		this.fetcher = fetcher;
		this.subNode = subNode;
		this.arrayType = createArrayType(subNode);
		this.refreshable = subNode.isRefreshable();
		this.nameToOpenType = createMapWithOneEntry(name, arrayType);
	}

	private static ArrayType<?> createArrayType(AttributeNode subNode) {
		try {
			return new ArrayType<>(1, subNode.getOpenType());
		} catch (OpenDataException e) {
			throw new RuntimeException(e);
		}
	}

	@Override
	public Map<String, OpenType<?>> getFlattenedOpenTypes() {
		return nameToOpenType;
	}

	@Override
	public Map<String, Object> aggregateAllAttributes(List<?> sources) {
		Map<String, Object> attrs = new HashMap<>(1);
		attrs.put(name, aggregateAttribute(name, sources));
		return attrs;
	}

	@Override
	public String getName() {
		return name;
	}

	@Override
	public OpenType<?> getOpenType() {
		return arrayType;
	}

	@Override
	public Object aggregateAttribute(String attrName, List<?> sources) {
		checkArgument(attrName.equals(name));
		checkNotNull(sources);
		List<?> notNullSources = filterNulls(sources);
		if (notNullSources.size() == 0) {
			return null;
		}

		List<Map<String, Object>> attributesFromAllElements = new ArrayList<>();
		for (Object source : notNullSources) {
			List<?> currentList = (List<?>) fetcher.fetchFrom(source);
			if (currentList != null) {
				for (Object element : currentList) {
					Map<String, Object> attributesFromElement = subNode.aggregateAllAttributes(asList(element));
					attributesFromAllElements.add(attributesFromElement);
				}
			}
		}

		return createArrayFrom(attributesFromAllElements);
	}

	private Object[] createArrayFrom(List<Map<String, Object>> attributesFromAllElements) {
		OpenType<?> arrayElementOpenType = arrayType.getElementOpenType();
		if (arrayElementOpenType instanceof ArrayType) {
			// TODO(vmykhalko): add support for multidimensional arrays
			throw new RuntimeException("Multidimensional arrays are not supported");
		} else {
			try {
				Class<?> elementClass = classOf(arrayType.getElementOpenType());
				Object[] array = (Object[]) Array.newInstance(elementClass, attributesFromAllElements.size());
				for (int i = 0; i < attributesFromAllElements.size(); i++) {
					Map<String, Object> attributesFromElement = attributesFromAllElements.get(i);
					array[i] = jmxCompatibleObjectOf(arrayElementOpenType, attributesFromElement);
				}
				return array;
			} catch (OpenDataException e) {
				throw new RuntimeException(e);
			}
		}
	}

	private static Object jmxCompatibleObjectOf(OpenType<?> openType, Map<String, Object> attributes)
			throws OpenDataException {
		if (openType instanceof SimpleType || openType instanceof TabularType) {
			checkArgument(attributes.size() == 1);
			return attributes.values().iterator().next();
		} else if (openType instanceof CompositeType) {
			CompositeType compositeType = (CompositeType) openType;
			return new CompositeDataSupport(compositeType, attributes);
		} else {
			throw new RuntimeException("There is no support for " + openType);
		}
	}

	@Override
	public void refresh(List<?> targets, long timestamp, double smoothingWindow) {
		if (refreshable) {
			for (Object pojo : targets) {
				List<?> currentList = (List<?>) fetcher.fetchFrom(pojo);
				if (currentList != null) {
					for (Object element : currentList) {
						subNode.refresh(asList(element), timestamp, smoothingWindow);
					}
				}
			}
		}
	}

	@Override
	public boolean isRefreshable() {
		return refreshable;
	}
}
