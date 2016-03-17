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
import java.util.*;

import static io.datakernel.jmx.Utils.filterNulls;
import static io.datakernel.util.Preconditions.checkArgument;
import static io.datakernel.util.Preconditions.checkNotNull;
import static java.util.Arrays.asList;

// TODO(vmykhalko): pojoNode and jmxStatsNode seem to have a lot in common. Maybe extract abstract class ?
final class AttributeNodeForJmxStats implements AttributeNode {
	private static final String ATTRIBUTE_NAME_SEPARATOR = "_";
	private static final String COMPOSITE_TYPE_DEFAULT_NAME = "CompositeType";

	private final String name;
	private final Map<String, AttributeNode> nameToSubNode;
	private final ValueFetcher fetcher;
	private final Class<?> jmxStatsClass;
	private final CompositeType compositeType;
	private final Map<String, OpenType<?>> nameToOpenType;
	private final Map<String, AttributeNode> fullNameToNode;

	public AttributeNodeForJmxStats(String name, ValueFetcher fetcher, Class<?> jmxStatsClass,
	                                List<? extends AttributeNode> subNodes) {
		checkArgument(!name.isEmpty(), "JmxStats attribute cannot have empty name");

		this.name = name;
		this.fetcher = checkNotNull(fetcher);
		this.jmxStatsClass = checkNotNull(jmxStatsClass);
		this.compositeType = createCompositeType(name, subNodes);
		this.nameToOpenType = createNameToOpenTypeMap(name, subNodes);

		nameToSubNode = new HashMap<>(subNodes.size());
		for (AttributeNode subNode : subNodes) {
			checkNotNull(subNode);
			String subNodeName = checkNotNull(subNode.getName());
			checkArgument(!subNodeName.isEmpty(), "In JmxStats empty attribute names are not allowed");
			nameToSubNode.put(subNodeName, subNode);
		}

		this.fullNameToNode = createFullNameToNodeMapping(name, subNodes);

	}

	private static Map<String, AttributeNode> createFullNameToNodeMapping(String name,
	                                                                      List<? extends AttributeNode> subNodes) {
		Map<String, AttributeNode> fullNameToNodeMapping = new HashMap<>();
		for (AttributeNode subNode : subNodes) {
			Set<String> currentSubAttrNames = subNode.getFlattenedOpenTypes().keySet();
			for (String currentSubAttrName : currentSubAttrNames) {
				String prefix = name.isEmpty() ? "" : name + "_";
				String currentAttrFullName = prefix + currentSubAttrName;
				if (fullNameToNodeMapping.containsKey(currentAttrFullName)) {
					throw new IllegalArgumentException(
							"There are several attributes with same name: " + currentSubAttrName);
				}
				fullNameToNodeMapping.put(currentAttrFullName, subNode);
			}
		}
		return fullNameToNodeMapping;
	}

	private static Map<String, OpenType<?>> createNameToOpenTypeMap(String nodeName,
	                                                                List<? extends AttributeNode> subNodes) {
		Map<String, OpenType<?>> nameToOpenType = new HashMap<>();
		String prefix = nodeName.isEmpty() ? "" : nodeName + ATTRIBUTE_NAME_SEPARATOR;
		for (AttributeNode subNode : subNodes) {
			Map<String, OpenType<?>> currentSubNodeMap = subNode.getFlattenedOpenTypes();
			for (String subNodeAttrName : currentSubNodeMap.keySet()) {
				nameToOpenType.put(prefix + subNodeAttrName, currentSubNodeMap.get(subNodeAttrName));
			}
		}
		return nameToOpenType;
	}

	private static CompositeType createCompositeType(String name, List<? extends AttributeNode> subNodes) {
		List<String> itemNames = new ArrayList<>();
		List<OpenType<?>> itemTypes = new ArrayList<>();
		String prefix = name.isEmpty() ? "" : name + ATTRIBUTE_NAME_SEPARATOR;
		for (AttributeNode subNode : subNodes) {
			Map<String, OpenType<?>> subNodeFlattenedTypes = subNode.getFlattenedOpenTypes();
			for (String attrName : subNodeFlattenedTypes.keySet()) {
				itemNames.add(prefix + attrName);
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
	public Map<String, OpenType<?>> getFlattenedOpenTypes() {
		return nameToOpenType;
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
	public Map<String, Object> aggregateAllAttributes(List<?> sources) {
		JmxStats accumulator = null;
		try {
			accumulator = (JmxStats) jmxStatsClass.newInstance();
		} catch (InstantiationException | IllegalAccessException e) {
			throw new RuntimeException(e);
		}
		for (Object pojo : sources) {
			accumulator.add((JmxStats) fetcher.fetchFrom(pojo));
		}

		Map<String, Object> attrs = new HashMap<>();
		List<?> accumulatorInList = asList(accumulator);
		String prefix = name.isEmpty() ? "" : name + ATTRIBUTE_NAME_SEPARATOR;
		for (AttributeNode attributeNode : nameToSubNode.values()) {
			Map<String, Object> subAttrs = attributeNode.aggregateAllAttributes(accumulatorInList);
			for (String subAttrName : subAttrs.keySet()) {
				attrs.put(prefix + subAttrName, subAttrs.get(subAttrName));
			}
		}
		return attrs;

	}

	@Override
	public Object aggregateAttribute(String attrName, List<?> sources) {
		Map<String, Object> allAttrs = aggregateAllAttributes(sources);
		if (!allAttrs.containsKey(attrName)) {
			throw new IllegalArgumentException("There is no attribute with name: " + attrName);
		}
		return allAttrs.get(attrName);
	}

	@Override
	public void refresh(List<?> targets, long timestamp) {
		for (Object pojo : targets) {
			((JmxStats<?>) fetcher.fetchFrom(pojo)).refreshStats(timestamp);
		}
	}

	@Override
	public boolean isRefreshable() {
		return true;
	}

	@Override
	public boolean isSettable(String attrName) {
		if (!fullNameToNode.containsKey(attrName)) {
			throw new IllegalArgumentException("There is no attribute with name: " + attrName);
		}

		AttributeNode appropriateSubNode = fullNameToNode.get(attrName);

		String subAttrName;
		if (name.isEmpty()) {
			subAttrName = attrName;
		} else {
			checkArgument(attrName.contains(ATTRIBUTE_NAME_SEPARATOR));
			int indexOfSeparator = attrName.indexOf(ATTRIBUTE_NAME_SEPARATOR);
			subAttrName = attrName.substring(indexOfSeparator + 1, attrName.length());
		}

		return appropriateSubNode.isSettable(subAttrName);
	}

	@Override
	public void setAttribute(String attrName, Object value, List<?> targets) {
		checkNotNull(targets);
		List<?> notNullTargets = filterNulls(targets);
		if (notNullTargets.size() == 0) {
			return;
		}

		if (!fullNameToNode.containsKey(attrName)) {
			throw new IllegalArgumentException("There is no attribute with name: " + attrName);
		}

		AttributeNode appropriateSubNode = fullNameToNode.get(attrName);

		if (name.isEmpty()) {
			appropriateSubNode.setAttribute(attrName, value, fetchInnerPojos(targets));
		} else {
			checkArgument(attrName.contains(ATTRIBUTE_NAME_SEPARATOR));
			int indexOfSeparator = attrName.indexOf(ATTRIBUTE_NAME_SEPARATOR);
			String subAttrName = attrName.substring(indexOfSeparator + 1, attrName.length());
			appropriateSubNode.setAttribute(subAttrName, value, fetchInnerPojos(targets));
		}
	}

	private List<Object> fetchInnerPojos(List<?> outerPojos) {
		List<Object> innerPojos = new ArrayList<>(outerPojos.size());
		for (Object outerPojo : outerPojos) {
			innerPojos.add(fetcher.fetchFrom(outerPojo));
		}
		return innerPojos;
	}
}
