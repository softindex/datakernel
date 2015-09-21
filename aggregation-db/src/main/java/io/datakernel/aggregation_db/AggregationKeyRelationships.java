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

package io.datakernel.aggregation_db;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;

import java.util.*;

import static com.google.common.collect.Sets.newHashSet;

public class AggregationKeyRelationships {
	private final Map<String, String> parentChildRelationships;
	private final Multimap<String, String> childParentRelationships;

	public AggregationKeyRelationships(Map<String, String> parentChildRelationships) {
		this.parentChildRelationships = parentChildRelationships;
		this.childParentRelationships = HashMultimap.create();
		for (Map.Entry<String, String> parentChildEntry : parentChildRelationships.entrySet()) {
			String parent = parentChildEntry.getKey();
			String child = parentChildEntry.getValue();
			childParentRelationships.put(child, parent);
		}
	}

	public Set<List<String>> buildDrillDownChains(Set<String> usedDimensions, Set<String> availableDimensions) {
		Set<List<String>> drillDowns = newHashSet();
		for (String dimension : availableDimensions) {
			List<String> drillDown = buildDrillDownChain(usedDimensions, dimension);
			drillDowns.add(drillDown);
		}
		return drillDowns;
	}

	public List<String> buildDrillDownChain(Set<String> usedDimensions, String dimension) {
		LinkedList<String> drillDown = new LinkedList<>();
		drillDown.add(dimension);
		String child = dimension;
		String parent;
		while ((parent = parentChildRelationships.get(child)) != null && !usedDimensions.contains(parent)) {
			drillDown.addFirst(parent);
			child = parent;
		}
		return drillDown;
	}

	public Set<String> findChildren(String parent) {
		Set<String> children = newHashSet();
		findChildren(children, parent);
		return children;
	}

	private void findChildren(Set<String> children, String parent) {
		Collection<String> childrenOfParent = childParentRelationships.get(parent);
		children.addAll(childrenOfParent);
		for (String child : childrenOfParent)
			findChildren(children, child);
	}
}
