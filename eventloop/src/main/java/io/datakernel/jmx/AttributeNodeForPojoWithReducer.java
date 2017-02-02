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

import static io.datakernel.jmx.Utils.filterNulls;
import static io.datakernel.util.Preconditions.checkArgument;
import static io.datakernel.util.Preconditions.checkNotNull;
import static java.util.Arrays.asList;

final class AttributeNodeForPojoWithReducer extends AttributeNodeForPojoAbstract {
	private final JmxReducer reducer;
	private final List<? extends AttributeNode> subNodes;
	private final Cache<Map<String, Object>> cache;

	public AttributeNodeForPojoWithReducer(String name, String description, boolean visible, ValueFetcher fetcher,
	                                       JmxReducer reducer, List<? extends AttributeNode> subNodes,
	                                       Cache<Map<String, Object>> cache) {
		super(name, description, visible, fetcher, subNodes);

		this.reducer = checkNotNull(reducer);
		this.subNodes = subNodes;
		this.cache = cache;
	}

	@Override
	@SuppressWarnings("unchecked")
	public final Map<String, Object> aggregateAllAttributes(List<?> sources) {
		List<Object> pojos = new ArrayList<>();
		for (Object source : sources) {
			Object fetched = fetcher.fetchFrom(source);
			if (fetched != null) {
				pojos.add(fetched);
			}
		}

		Object reducedPojo = reducer.reduce(pojos);
		List<?> reducedPojoWrappedInList = asList(reducedPojo);
		Map<String, Object> attrs = new HashMap<>();
		String prefix = name.isEmpty() ? "" : name + ATTRIBUTE_NAME_SEPARATOR;
		for (AttributeNode subNode : subNodes) {
			if (subNode.isVisible()) {
				Map<String, Object> subNodeAttrs = subNode.aggregateAllAttributes(reducedPojoWrappedInList);
				for (String subAttrName : subNodeAttrs.keySet()) {
					attrs.put(prefix + subAttrName, subNodeAttrs.get(subAttrName));
				}
			}
		}
		return attrs;
	}

	@Override
	public final Object aggregateAttribute(String attrName, List<?> sources) {
		checkNotNull(sources);
		List<?> notNullSources = filterNulls(sources);
		if (notNullSources.size() == 0) {
			return null;
		}

		if (notNullSources.size() == 1) {
			// TODO(vmykhalko): refactor - extract common code
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

		// TODO(vmykhalko): extract common code from pojo with reducer and jmx stats (maybe implicitly convert JmxStats into JmxReducer)
		if (!cache.isExpired()) {
			return cache.getValue().get(attrName);
		}

		Map<String, Object> allAttrs = aggregateAllAttributes(sources);
		if (!allAttrs.containsKey(attrName)) {
			throw new IllegalArgumentException("There is no attribute with name: " + attrName);
		}

		cache.update(allAttrs);

		return allAttrs.get(attrName);
	}

	@Override
	public Iterable<JmxRefreshable> getAllRefreshables(Object source) {
		// TODO(vmykhalko): implement
		return null;
	}

	@Override
	protected AttributeNode recreate(List<? extends AttributeNode> subNodes, boolean visible) {
		return new AttributeNodeForPojoWithReducer(name, description, visible, fetcher, reducer, subNodes, cache);
	}
}
