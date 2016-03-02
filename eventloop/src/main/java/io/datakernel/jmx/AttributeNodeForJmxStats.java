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
import static java.util.Arrays.asList;

// TODO(vmykhalko): pojoNode and jmxStatsNode seem to have a lot in common. Maybe extract abstract class ?
final class AttributeNodeForJmxStats implements AttributeNode {
	private static final String COMPOSITE_TYPE_DEFAULT_NAME = "CompositeType";

	private final String name;
	private final Map<String, AttributeNode> nameToSubNode;
	private final ValueFetcher fetcher;
	private final Class<?> jmxStatsClass;
	private final CompositeType compositeType;
	private final Map<String, OpenType<?>> nameToOpenType;

	public AttributeNodeForJmxStats(String name, ValueFetcher fetcher, Class<?> jmxStatsClass,
	                                List<? extends AttributeNode> subNodes) {
		checkArgument(!name.isEmpty());
		this.name = name;
		this.fetcher = checkNotNull(fetcher);
		this.jmxStatsClass = checkNotNull(jmxStatsClass);
		this.compositeType = createCompositeType(subNodes);
		this.nameToOpenType = createNameToOpenTypeMap(name, subNodes);

		nameToSubNode = new HashMap<>(subNodes.size());
		for (AttributeNode subNode : subNodes) {
			checkNotNull(subNode);
			String subNodeName = checkNotNull(subNode.getName());
			checkArgument(!subNodeName.isEmpty(), "In JmxStats empty attribute names are not allowed");
			nameToSubNode.put(subNodeName, subNode);
		}

	}

	private static Map<String, OpenType<?>> createNameToOpenTypeMap(String nodeName,
	                                                                List<? extends AttributeNode> subNodes) {
		Map<String, OpenType<?>> nameToOpenType = new HashMap<>();
		String prefix = nodeName.isEmpty() ? "" : nodeName + "_";
		for (AttributeNode subNode : subNodes) {
			Map<String, OpenType<?>> currentSubNodeMap = subNode.getFlattenedOpenTypes();
			for (String subNodeAttrName : currentSubNodeMap.keySet()) {
				nameToOpenType.put(prefix + subNodeAttrName, currentSubNodeMap.get(subNodeAttrName));
			}
		}
		return nameToOpenType;
	}

	private static CompositeType createCompositeType(List<? extends AttributeNode> subNodes) {
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
		String prefix = name.isEmpty() ? "" : name + "_";
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
		try {
			Map<String, Object> allAttrs = aggregateAllAttributes(sources);
			if (!allAttrs.containsKey(attrName)) {
				throw new IllegalArgumentException("There is no attribute with name: " + attrName);
			}
			return allAttrs.get(attrName);
		} catch (Exception e) {
			e.printStackTrace();
			throw new RuntimeException(e);
		}
	}

	@Override
	public void refresh(List<?> targets, long timestamp, double smoothingWindow) {
		for (Object pojo : targets) {
			((JmxStats<?>) fetcher.fetchFrom(pojo)).refreshStats(timestamp, smoothingWindow);
		}
	}

	@Override
	public boolean isRefreshable() {
		return true;
	}
}
