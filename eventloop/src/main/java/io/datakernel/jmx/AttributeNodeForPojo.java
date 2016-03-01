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

import javax.management.openmbean.CompositeType;
import javax.management.openmbean.OpenDataException;
import javax.management.openmbean.OpenType;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static io.datakernel.util.Preconditions.checkArgument;
import static io.datakernel.util.Preconditions.checkNotNull;

final class AttributeNodeForPojo implements AttributeNode {
	private static final String ATTRIBUTE_NAME_SEPARATOR = "_";
	private static final String COMPOSITE_TYPE_DEFAULT_NAME = "CompositeType";

	private final String name;
	private final Map<String, AttributeNode> nameToSubNode;
	private final List<AttributeNode> refreshableSubNodes;
	private final ValueFetcher fetcher;
	private final CompositeType compositeType;
	private final Map<String, OpenType<?>> nameToOpenType;
	private final boolean refreshable;

	// TODO(vmykhalko): treat "" as absence of node name
	public AttributeNodeForPojo(String name, ValueFetcher fetcher, List<? extends AttributeNode> subNodes) {
		this.name = name;
		this.fetcher = fetcher;
		this.compositeType = createCompositeType(subNodes);
		this.nameToOpenType = createNameToOpenTypeMap(name, subNodes);
		this.refreshableSubNodes = filterRefreshableSubNodes(subNodes);
		this.refreshable = refreshableSubNodes.size() > 0;

		nameToSubNode = new HashMap<>(subNodes.size());
		if (subNodes.size() == 1 && subNodes.get(0).getName().equals("")) {
			nameToSubNode.put("", subNodes.get(0));
		} else {
			for (AttributeNode subNode : subNodes) {
				checkNotNull(subNode);
				String subNodeName = checkNotNull(subNode.getName());
				checkArgument(!subNodeName.isEmpty(), "Attribute name can be empty only in case of one attribute per class");
				nameToSubNode.put(subNodeName, subNode);
			}
		}
	}

	private static List<AttributeNode> filterRefreshableSubNodes(List<? extends AttributeNode> subNodes) {
		List<AttributeNode> refreshableSubNodes = new ArrayList<>();
		for (AttributeNode subNode : subNodes) {
			if (subNode.isRefreshable()) {
				refreshableSubNodes.add(subNode);
			}
		}
		return refreshableSubNodes;
	}

	private static Map<String, OpenType<?>> createNameToOpenTypeMap(String nodeName,
	                                                                List<? extends AttributeNode> subNodes) {
		Map<String, OpenType<?>> nameToOpenType = new HashMap<>();
		for (AttributeNode subNode : subNodes) {
			Map<String, OpenType<?>> currentSubNodeMap = subNode.getFlattenedOpenTypes();
			for (String subNodeAttrName : currentSubNodeMap.keySet()) {
				String prefix = nodeName.isEmpty() ? "" : nodeName + "_";
				nameToOpenType.put(prefix + subNodeAttrName, currentSubNodeMap.get(subNodeAttrName));
			}
		}
		return nameToOpenType;
	}

	private static CompositeType createCompositeType(List<? extends AttributeNode> subNodes) {
		if (subNodes.size() == 0) {
			return null;
		}
		List<String> itemNames = new ArrayList<>();
		List<OpenType<?>> itemTypes = new ArrayList<>();
		for (AttributeNode subNode : subNodes) {
			Map<String, OpenType<?>> subNodeFlattenedTypes = subNode.getFlattenedOpenTypes();
			for (String attrName : subNodeFlattenedTypes.keySet()) {
				itemNames.add(attrName);
				itemTypes.add(subNodeFlattenedTypes.get(attrName));
			}
		}
		String[] itemNamesArr = itemNames.toArray(new String[itemNames.size()]);
		OpenType<?>[] itemTypesArr = itemTypes.toArray(new OpenType<?>[itemTypes.size()]);
		try {
			return new CompositeType(
					COMPOSITE_TYPE_DEFAULT_NAME, COMPOSITE_TYPE_DEFAULT_NAME,
					itemNamesArr, itemNamesArr,
					itemTypesArr);
		} catch (OpenDataException e) {
			throw new RuntimeException(e);
		}
	}

	@Override
	public String getName() {
		return name;
	}

	@Override
	public OpenType<?> getOpenType() {
		return compositeType;
	}

	@Override
	public Map<String, OpenType<?>> getFlattenedOpenTypes() {
		return nameToOpenType;
	}

	@Override
	public Map<String, Object> aggregateAllAttributes(List<?> pojos) {
		Map<String, Object> attrs = new HashMap<>();
		List<Object> innerPojos = fetchInnerPojos(pojos);
		String prefix = name.isEmpty() ? "" : name + "_";
		for (AttributeNode attributeNode : nameToSubNode.values()) {
			Map<String, Object> subAttrs = attributeNode.aggregateAllAttributes(innerPojos);
			for (String subAttrName : subAttrs.keySet()) {
				attrs.put(prefix + subAttrName, subAttrs.get(subAttrName));
			}
		}
		return attrs;
	}

	@Override
	public Object aggregateAttribute(List<?> pojos, String attrName) {
		if (nameToSubNode.size() == 1 && nameToSubNode.containsKey("")) {
			return nameToSubNode.values().iterator().next().aggregateAttribute(fetchInnerPojos(pojos), attrName);
		}

		String attrGroupName = attrName;
		String subAttrName = null;
		if (attrName.contains(ATTRIBUTE_NAME_SEPARATOR)) {
			int indexOfSeparator = attrName.indexOf(ATTRIBUTE_NAME_SEPARATOR);
			attrGroupName = attrName.substring(0, indexOfSeparator);
			subAttrName = attrName.substring(indexOfSeparator + 1, attrName.length());
		}

		if (!nameToSubNode.containsKey(attrGroupName)) {
			throw new IllegalArgumentException("There is no attribute with name: " + attrGroupName);
		}

		return nameToSubNode.get(attrGroupName).aggregateAttribute(fetchInnerPojos(pojos), subAttrName);
	}

	private List<Object> fetchInnerPojos(List<?> outerPojos) {
		List<Object> innerPojos = new ArrayList<>(outerPojos.size());
		for (Object outerPojo : outerPojos) {
			innerPojos.add(fetcher.fetchFrom(outerPojo));
		}
		return innerPojos;
	}

	@Override
	public void refresh(List<?> pojos, long timestamp, double smoothingWindow) {
		List<Object> innerPojos = fetchInnerPojos(pojos);
		for (AttributeNode refreshableSubNode : refreshableSubNodes) {
			refreshableSubNode.refresh(innerPojos, timestamp, smoothingWindow);
		}
	}

	@Override
	public boolean isRefreshable() {
		return refreshable;
	}
}
