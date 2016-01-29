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

public abstract class MapStats<K, S extends JmxStats<S>> implements JmxStats<MapStats<K, S>>, Map<K, S> {
	private static final String ATTRIBUTE_NAME = "mapStats";
	private static final String KEY_NAME = "_key";

	private Map<K, S> keyToStats = new HashMap<>();

	private SortedMap<String, TypeAndValue> attributesOfInnerJmxStats;

	protected abstract S createJmxStatsInstance();

	@Override
	public void add(MapStats<K, S> other) {
		Set<K> allOwnKeys = this.keyToStats.keySet();
		Set<K> allOtherKeys = other.keyToStats.keySet();

		// intersection
		Set<K> commonKeys = new HashSet<>(allOwnKeys);
		commonKeys.retainAll(allOtherKeys);

		Set<K> uniqueOtherKeys = new HashSet<>(allOtherKeys);
		uniqueOtherKeys.removeAll(allOwnKeys);

		// add stats of unique other keys
		for (K uniqueOtherKey : uniqueOtherKeys) {
			// create new instance to avoid side-effects
			S accumulator = createJmxStatsInstance();
			accumulator.add(other.keyToStats.get(uniqueOtherKey));
			keyToStats.put(uniqueOtherKey, accumulator);
		}

		// aggregate stats of common keys
		for (K commonKey : commonKeys) {
			keyToStats.get(commonKey).add(other.keyToStats.get(commonKey));
		}
	}

	@Override
	public void refreshStats(long timestamp, double smoothingWindow) {
		for (S stats : keyToStats.values()) {
			stats.refreshStats(timestamp, smoothingWindow);
		}
	}

	@Override
	public SortedMap<String, TypeAndValue> getAttributes() {
		if (attributesOfInnerJmxStats == null) {
			attributesOfInnerJmxStats = createJmxStatsInstance().getAttributes();
		}
		try {
			SortedMap<String, TypeAndValue> distributedStatsAttributes = new TreeMap<>();
			CompositeType rowType = createRowType();
			TabularType tabularType =
					new TabularType("MapStats", "MapStats", rowType, new String[]{KEY_NAME});

			TabularDataSupport tdSupport = new TabularDataSupport(tabularType);
			for (K key : keyToStats.keySet()) {
				SortedMap<String, TypeAndValue> currentStatsAttributes = keyToStats.get(key).getAttributes();
				List<String> itemNames = new ArrayList<>();
				List<Object> itemValues = new ArrayList<>();
				itemNames.add(KEY_NAME);
				itemValues.add(key.toString());
				for (String currentAttrName : currentStatsAttributes.keySet()) {
					itemNames.add(currentAttrName);
					itemValues.add(currentStatsAttributes.get(currentAttrName).getValue());
				}
				String[] itemNamesArr = itemNames.toArray(new String[itemNames.size()]);
				Object[] itemValuesArr = itemValues.toArray(new Object[itemValues.size()]);
				CompositeDataSupport cdSupport = new CompositeDataSupport(rowType, itemNamesArr, itemValuesArr);
				tdSupport.put(cdSupport);
			}

			distributedStatsAttributes.put(ATTRIBUTE_NAME, new TypeAndValue(tabularType, tdSupport));
			return distributedStatsAttributes;
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
	}

	private CompositeType createRowType() throws OpenDataException {
		List<String> columnNames = new ArrayList<>();
		List<OpenType<?>> columnTypes = new ArrayList<>();
		columnNames.add(KEY_NAME);
		columnTypes.add(SimpleType.STRING);
		if (attributesOfInnerJmxStats == null) {
			attributesOfInnerJmxStats = createJmxStatsInstance().getAttributes();
		}
		for (String attrName : attributesOfInnerJmxStats.keySet()) {
			columnNames.add(attrName);
			columnTypes.add(attributesOfInnerJmxStats.get(attrName).getType());
		}
		String[] columnNamesArr = columnNames.toArray(new String[columnNames.size()]);
		OpenType<?>[] columnTypesArr = columnTypes.toArray(new OpenType<?>[columnTypes.size()]);
		return new CompositeType("rowType", "rowType", columnNamesArr, columnNamesArr, columnTypesArr);
	}

	// Map methods
	@Override
	public int size() {
		return keyToStats.size();
	}

	@Override
	public boolean isEmpty() {
		return keyToStats.isEmpty();
	}

	@Override
	public boolean containsKey(Object key) {
		return keyToStats.containsKey(key);
	}

	@Override
	public boolean containsValue(Object value) {
		return keyToStats.containsValue(value);
	}

	@Override
	public S get(Object key) {
		return keyToStats.get(key);
	}

	@Override
	public S put(K key, S value) {
		return keyToStats.put(key, value);
	}

	@Override
	public S remove(Object key) {
		return keyToStats.remove(key);
	}

	@Override
	public void putAll(Map<? extends K, ? extends S> m) {
		keyToStats.putAll(m);
	}

	@Override
	public void clear() {
		keyToStats.clear();
	}

	@Override
	public Set<K> keySet() {
		return keyToStats.keySet();
	}

	@Override
	public Collection<S> values() {
		return keyToStats.values();
	}

	@Override
	public Set<Entry<K, S>> entrySet() {
		return keyToStats.entrySet();
	}
}
