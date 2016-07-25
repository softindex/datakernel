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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static io.datakernel.jmx.Utils.concat;
import static io.datakernel.jmx.Utils.filterNulls;
import static io.datakernel.util.Preconditions.checkArgument;
import static io.datakernel.util.Preconditions.checkNotNull;

final class AttributeNodeForPojo extends AttributeNodeForPojoAbstract {
	private final List<? extends AttributeNode> subNodes;

	public AttributeNodeForPojo(String name, ValueFetcher fetcher, List<? extends AttributeNode> subNodes) {
		super(name, fetcher, subNodes);
		this.subNodes = subNodes;
	}

	@Override
	public Map<String, Object> aggregateAllAttributes(List<?> sources) {
		Map<String, Object> allAttrs = new HashMap<>();
		for (String attrFullName : fullNameToNode.keySet()) {
			allAttrs.put(attrFullName, aggregateAttribute(attrFullName, sources));
		}
		return allAttrs;
	}

	@Override
	public Object aggregateAttribute(String attrName, List<?> sources) {
		checkNotNull(sources);
		List<?> notNullSources = filterNulls(sources);
		if (notNullSources.size() == 0) {
			return null;
		}

		if (!fullNameToNode.containsKey(attrName)) {
			throw new IllegalArgumentException("There is no attribute with name: " + attrName);
		}

		AttributeNode appropriateSubNode = fullNameToNode.get(attrName);

		if (name.isEmpty()) {
			return appropriateSubNode.aggregateAttribute(attrName, fetchInnerPojos(notNullSources));
		} else {
			checkArgument(attrName.contains(ATTRIBUTE_NAME_SEPARATOR));
			int indexOfSeparator = attrName.indexOf(ATTRIBUTE_NAME_SEPARATOR);
			String subAttrName = attrName.substring(indexOfSeparator + 1, attrName.length());
			return appropriateSubNode.aggregateAttribute(subAttrName, fetchInnerPojos(notNullSources));
		}
	}

	@Override
	public Iterable<JmxRefreshable> getAllRefreshables(Object source) {
		Object pojo = fetcher.fetchFrom(source);
		List<Iterable<JmxRefreshable>> listOfIterators = new ArrayList<>();
		for (AttributeNode attributeNode : subNodes) {
			Iterable<JmxRefreshable> iterable = attributeNode.getAllRefreshables(pojo);
			if (iterable != null) {
				listOfIterators.add(iterable);
			}
		}
		return concat(listOfIterators);
	}
}
