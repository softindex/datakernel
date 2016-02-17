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
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static io.datakernel.util.Preconditions.checkNotNull;

final class PojoAttributeNode extends AbstractAttributeNode {
	private static final String ATTRIBUTE_NAME_SEPARATOR = "_";
	private final Map<String, AttributeNode> nameToSubNode;

	public PojoAttributeNode(String name, Method getter, CompositeType compositeType,
	                         List<? extends AttributeNode> subNodes) {
		super(name, getter, compositeType);

		nameToSubNode = new HashMap<>(subNodes.size());
		for (AttributeNode subNode : subNodes) {
			checkNotNull(subNode);
			String subNodeName = checkNotNull(subNode.getName());
			nameToSubNode.put(subNodeName, subNode);
		}
	}

	@Override
	public Map<String, Object> aggregateAllAttributes(List<?> pojos) {
		Map<String, Object> attrs = new HashMap<>();
		List<Object> innerPojos = fetchInnerPojos(pojos);
		String groupName = getName();
		for (AttributeNode attributeNode : nameToSubNode.values()) {
			Map<String, Object> subAttrs = attributeNode.aggregateAllAttributes(innerPojos);
			for (String subAttrName : subAttrs.keySet()) {
				attrs.put(groupName + subAttrName, subAttrs.get(subAttrName));
			}
		}
		return attrs;
	}

	@Override
	public Object aggregateAttribute(List<?> pojos, String attrName) {
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
		List<Object> innerPojos = new ArrayList<>(outerPojos);
		for (Object outerPojo : outerPojos) {
			innerPojos.add(fetchValueFrom(outerPojo));
		}
		return innerPojos;
	}
}
