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

import io.datakernel.jmx.api.JmxRefreshable;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import javax.management.openmbean.*;
import java.lang.reflect.Array;
import java.util.*;

import static io.datakernel.common.Preconditions.checkArgument;
import static io.datakernel.common.collection.CollectionUtils.first;
import static java.lang.String.format;
import static java.util.Collections.emptyList;
import static java.util.Collections.singletonList;

final class AttributeNodeForList extends AttributeNodeForLeafAbstract {
	private final AttributeNode subNode;
	private final ArrayType<?> arrayType;
	private final boolean isListOfJmxRefreshables;

	public AttributeNodeForList(String name, @Nullable String description, boolean visible, ValueFetcher fetcher, AttributeNode subNode,
			boolean isListOfJmxRefreshables) {
		super(name, description, fetcher, visible);
		checkArgument(!name.isEmpty(), "List attribute cannot have empty name");
		this.subNode = subNode;
		this.arrayType = createArrayType(subNode, name);
		this.isListOfJmxRefreshables = isListOfJmxRefreshables;
	}

	private static ArrayType<?> createArrayType(AttributeNode subNode, String name) {
		String nodeName = "Attribute name = " + name;

		Set<String> visibleAttrs = subNode.getVisibleAttributes();
		Map<String, OpenType<?>> attrTypes = subNode.getOpenTypes();

		if (visibleAttrs.size() == 0) {
			throw new IllegalArgumentException("Arrays must have at least one visible attribute. " + nodeName);
		}

		try {
			OpenType<?> elementType;
			if (visibleAttrs.size() == 1) {
				String attr = first(visibleAttrs);
				OpenType<?> openType = attrTypes.get(attr);

				// TODO(vmykhalko): check this case
				if (openType instanceof ArrayType) {
					throw new IllegalArgumentException("Multidimensional arrays are note supported in JMX. " + nodeName);
				}

				elementType = openType;
			} else {
				List<String> itemNames = new ArrayList<>();
				List<OpenType<?>> itemTypes = new ArrayList<>();
				for (String visibleAttr : visibleAttrs) {
					OpenType<?> visibleAttrType = attrTypes.get(visibleAttr);
					itemNames.add(visibleAttr);
					itemTypes.add(visibleAttrType);
				}

				String[] itemNamesArr = itemNames.toArray(new String[0]);
				OpenType<?>[] itemTypesArr = itemTypes.toArray(new OpenType<?>[0]);
				elementType =
						new CompositeType("CompositeData", "CompositeData", itemNamesArr, itemNamesArr, itemTypesArr);
			}

			return new ArrayType<>(1, elementType);
		} catch (OpenDataException e) {
			throw new IllegalArgumentException("Cannot create ArrayType. " + nodeName, e);
		}
	}

	@Override
	public Map<String, OpenType<?>> getOpenTypes() {
		return Collections.singletonMap(name, arrayType);
	}

	@Nullable
	@Override
	public Object aggregateAttribute(String attrName, List<?> sources) {
		List<Map<String, Object>> attributesFromAllElements = new ArrayList<>();
		Set<String> visibleSubAttrs = subNode.getVisibleAttributes();
		for (Object source : sources) {
			List<?> currentList = (List<?>) fetcher.fetchFrom(source);
			if (currentList != null) {
				for (Object element : currentList) {
					Map<String, Object> attributesFromElement =
							subNode.aggregateAttributes(visibleSubAttrs, singletonList(element));

					attributesFromAllElements.add(attributesFromElement);
				}
			}
		}

		return attributesFromAllElements.size() > 0 ? createArrayFrom(attributesFromAllElements) : null;
	}

	private Object[] createArrayFrom(List<Map<String, Object>> attributesFromAllElements) {
		OpenType<?> arrayElementOpenType = arrayType.getElementOpenType();
		if (arrayElementOpenType instanceof ArrayType) {
			throw new RuntimeException("Multidimensional arrays are not supported");
		}
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

	private static Object jmxCompatibleObjectOf(OpenType<?> openType, Map<String, Object> attributes)
			throws OpenDataException {
		if (openType instanceof SimpleType || openType instanceof TabularType) {
			checkArgument(attributes.size() == 1, "Only one attribute should be present");
			return first(attributes.values());
		} else if (openType instanceof CompositeType) {
			CompositeType compositeType = (CompositeType) openType;
			return new CompositeDataSupport(compositeType, attributes);
		}
		throw new RuntimeException("There is no support for " + openType);
	}

	@Override
	@SuppressWarnings("unchecked")
	public List<JmxRefreshable> getAllRefreshables(@NotNull Object source) {
		if (!isListOfJmxRefreshables) {
			return emptyList();
		}

		List<JmxRefreshable> listRef = (List<JmxRefreshable>) fetcher.fetchFrom(source);
		return Collections.singletonList(timestamp -> {
			for (JmxRefreshable jmxRefreshableElement : listRef) {
				jmxRefreshableElement.refresh(timestamp);
			}
		});
	}

	@Override
	public boolean isSettable(@NotNull String attrName) {
		return false;
	}

	@Override
	public void setAttribute(@NotNull String attrName, @NotNull Object value, @NotNull List<?> targets) {
		throw new UnsupportedOperationException("Cannot set attributes for list attribute node");
	}

	private static Class<?> classOf(OpenType<?> openType) {
		if (openType.equals(SimpleType.BOOLEAN)) {
			return Boolean.class;
		} else if (openType.equals(SimpleType.BYTE)) {
			return Byte.class;
		} else if (openType.equals(SimpleType.SHORT)) {
			return Short.class;
		} else if (openType.equals(SimpleType.CHARACTER)) {
			return Character.class;
		} else if (openType.equals(SimpleType.INTEGER)) {
			return Integer.class;
		} else if (openType.equals(SimpleType.LONG)) {
			return Long.class;
		} else if (openType.equals(SimpleType.FLOAT)) {
			return Float.class;
		} else if (openType.equals(SimpleType.DOUBLE)) {
			return Double.class;
		} else if (openType.equals(SimpleType.STRING)) {
			return String.class;
		} else if (openType instanceof CompositeType) {
			return CompositeData.class;
		} else if (openType instanceof TabularType) {
			return TabularData.class;
		}
		// ArrayType is not supported
		throw new IllegalArgumentException(format("OpenType \"%s\" cannot be converted to Class", openType));
	}
}

