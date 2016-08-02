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
import java.util.*;

import static io.datakernel.jmx.Utils.*;
import static io.datakernel.util.Preconditions.checkArgument;
import static io.datakernel.util.Preconditions.checkNotNull;

final class AttributeNodeForMap implements AttributeNode {
	private static final String KEY_COLUMN_NAME = "> key";
	private static final String VALUE_COLUMN_NAME = "value";
	private static final String ROW_TYPE_NAME = "RowType";
	private static final String TABULAR_TYPE_NAME = "TabularType";

	private final String name;
	private final String description;
	private final ValueFetcher fetcher;
	private final AttributeNode subNode;
	private final TabularType tabularType;
	private final Map<String, OpenType<?>> nameToOpenType;
	private final boolean isMapOfJmxRefreshable;

	public AttributeNodeForMap(String name, String description, ValueFetcher fetcher, AttributeNode subNode,
	                           boolean isMapOfJmxRefreshable) {
		checkArgument(!name.isEmpty(), "Map attribute cannot have empty name");

		this.name = name;
		this.description = description;
		this.tabularType = createTabularType(subNode);
		this.nameToOpenType = createMapWithOneEntry(name, tabularType);
		this.fetcher = fetcher;
		this.subNode = subNode;
		this.isMapOfJmxRefreshable = isMapOfJmxRefreshable;
	}

	private static TabularType createTabularType(AttributeNode subNode) {
		List<String> columnNames = new ArrayList<>();
		List<OpenType<?>> columnTypes = new ArrayList<>();
		columnNames.add(KEY_COLUMN_NAME);
		columnTypes.add(SimpleType.STRING);
		OpenType<?> subNodeOpenType = subNode.getOpenType();
		if (subNodeOpenType instanceof CompositeType) {
			CompositeType subNodeCompositeType = (CompositeType) subNodeOpenType;
			for (String subNodeAttrName : subNodeCompositeType.keySet()) {
				columnNames.add(subNodeAttrName);
				columnTypes.add(subNodeCompositeType.getType(subNodeAttrName));
			}
		} else {
			columnNames.add(VALUE_COLUMN_NAME);
			columnTypes.add(subNodeOpenType);
		}
		String[] columnNamesArr = columnNames.toArray(new String[columnNames.size()]);
		OpenType<?>[] columnTypesArr = columnTypes.toArray(new OpenType<?>[columnTypes.size()]);

		try {
			return new TabularType(
					TABULAR_TYPE_NAME,
					TABULAR_TYPE_NAME,
					new CompositeType(ROW_TYPE_NAME, ROW_TYPE_NAME, columnNamesArr, columnNamesArr, columnTypesArr),
					new String[]{KEY_COLUMN_NAME}
			);
		} catch (OpenDataException e) {
			throw new RuntimeException(e);
		}
	}

	@Override
	public String getName() {
		return name;
	}

	@Override
	public Map<String, Map<String, String>> getDescriptions() {
		return createDescriptionMap(name, description);
	}

	@Override
	public OpenType<?> getOpenType() {
		return tabularType;
	}

	@Override
	public Map<String, OpenType<?>> getFlattenedOpenTypes() {
		return nameToOpenType;
	}

	@Override
	public Map<String, Object> aggregateAllAttributes(List<?> sources) {
		Map<String, Object> attrs = new HashMap<>();
		attrs.put(getName(), aggregateAttribute(getName(), sources));
		return attrs;
	}

	@Override
	public Object aggregateAttribute(String attrName, List<?> sources) {
		checkArgument(attrName.equals(name));
		checkNotNull(sources);
		List<?> notNullSources = filterNulls(sources);
		if (notNullSources.size() == 0) {
			return null;
		}

		Map<Object, List<Object>> groupedByKey = fetchMapsAndGroupEntriesByKey(sources);
		TabularDataSupport tdSupport = new TabularDataSupport(tabularType);
		for (Object key : groupedByKey.keySet()) {
			List<Object> group = groupedByKey.get(key);
			Map<String, Object> aggregatedGroup = subNode.aggregateAllAttributes(group);
			try {
				tdSupport.put(createTabularDataRow(key.toString(), aggregatedGroup));
			} catch (OpenDataException e) {
				throw new RuntimeException(e);
			}
		}
		return tdSupport;
	}

	private CompositeData createTabularDataRow(String key, Map<String, Object> attributes) throws OpenDataException {
		Map<String, Object> allAttributes = new HashMap<>(attributes.size() + 1);
		if (attributes.size() == 1 && tabularType.getRowType().containsKey("value")) {
			allAttributes.put(VALUE_COLUMN_NAME, attributes.values().iterator().next());
		} else {
			allAttributes.putAll(attributes);
		}
		allAttributes.put(KEY_COLUMN_NAME, key);
		return new CompositeDataSupport(tabularType.getRowType(), allAttributes);
	}

	private Map<Object, List<Object>> fetchMapsAndGroupEntriesByKey(List<?> pojos) {
		List<Map<?, ?>> listOfMaps = new ArrayList<>();
		for (Object pojo : pojos) {
			Map<?, ?> map = (Map<?, ?>) fetcher.fetchFrom(pojo);
			if (map != null && map.size() > 0) {
				listOfMaps.add(map);
			}
		}

		Map<Object, List<Object>> grouped = new HashMap<>();
		for (Map<?, ?> currentMap : listOfMaps) {
			for (Object key : currentMap.keySet()) {
				if (!grouped.containsKey(key)) {
					grouped.put(key, new ArrayList<>());
				}
				grouped.get(key).add(currentMap.get(key));
			}
		}
		return grouped;
	}

	@Override
	@SuppressWarnings("unchecked")
	public Iterable<JmxRefreshable> getAllRefreshables(final Object source) {
		if (!isMapOfJmxRefreshable) {
			return null;
		}

		final Map<?, JmxRefreshable> mapRef = ((Map<?, JmxRefreshable>) fetcher.fetchFrom(source));
		return new Iterable<JmxRefreshable>() {
			@Override
			public Iterator<JmxRefreshable> iterator() {
				Set mapValuesCopy = new HashSet(mapRef.values());
				return mapValuesCopy.iterator();
			}
		};
	}

	@Override
	public boolean isSettable(String attrName) {
		return false;
	}

	@Override
	public void setAttribute(String attrName, Object value, List<?> targets) {
		throw new UnsupportedOperationException();
	}
}
