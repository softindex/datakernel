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

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static io.datakernel.util.Preconditions.checkNotNull;
import static java.util.Arrays.asList;

abstract class AttributeNodeForJmxStatsAbstract extends AttributeNodeForPojoAbstract {
	private final Class<? extends JmxStats<?>> jmxStatsClass;
	private final List<? extends AttributeNode> subNodes;

	public AttributeNodeForJmxStatsAbstract(String name, ValueFetcher fetcher,
	                                        Class<? extends JmxStats<?>> jmxStatsClass,
	                                        List<? extends AttributeNode> subNodes) {
		super(name, fetcher, subNodes);

		this.jmxStatsClass = checkNotNull(jmxStatsClass);
		this.subNodes = subNodes;
	}

	@Override
	@SuppressWarnings("unchecked")
	public final Map<String, Object> aggregateAllAttributes(List<?> sources) {
		JmxStats accumulator;
		try {
			accumulator = jmxStatsClass.newInstance();
		} catch (InstantiationException | IllegalAccessException e) {
			throw new RuntimeException(e);
		}
		for (Object pojo : sources) {
			accumulator.add((JmxStats) fetcher.fetchFrom(pojo));
		}

		Map<String, Object> attrs = new HashMap<>();
		List<?> sinlgeAccumulatorWrappedInList = asList(accumulator);
		String prefix = name.isEmpty() ? "" : name + ATTRIBUTE_NAME_SEPARATOR;
		for (AttributeNode subNode : subNodes) {
			Map<String, Object> subNodeAttrs = subNode.aggregateAllAttributes(sinlgeAccumulatorWrappedInList);
			for (String subAttrName : subNodeAttrs.keySet()) {
				attrs.put(prefix + subAttrName, subNodeAttrs.get(subAttrName));
			}
		}
		return attrs;

	}

	@Override
	public final Object aggregateAttribute(String attrName, List<?> sources) {
		Map<String, Object> allAttrs = aggregateAllAttributes(sources);
		if (!allAttrs.containsKey(attrName)) {
			throw new IllegalArgumentException("There is no attribute with name: " + attrName);
		}
		return allAttrs.get(attrName);
	}
}
