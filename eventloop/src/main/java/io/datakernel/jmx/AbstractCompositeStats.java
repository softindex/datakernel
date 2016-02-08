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

import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;

import static io.datakernel.jmx.Utils.fetchNameToJmxStats;
import static java.lang.String.format;

// TODO (vmykhalko): according to new approach with POJO - maybe AbstractCompositeStats are useless
public abstract class AbstractCompositeStats<T extends AbstractCompositeStats<T>> implements JmxStats<T> {
	private static final String ATTRIBUTE_NAME_PATTERN = "%s_%s";

	@Override
	@SuppressWarnings("unchecked")
	public void add(T stats) {
		SortedMap<String, JmxStats<?>> thisAttributeToJmxStats = fetchNameToJmxStats(this);
		SortedMap<String, JmxStats<?>> otherAttributeToJmxStats = fetchNameToJmxStats(stats);
		for (String attributeKey : thisAttributeToJmxStats.keySet()) {
			// wildcard is removed intentionally, types must be same
			JmxStats currentThisJmxStats = thisAttributeToJmxStats.get(attributeKey);
			JmxStats<?> currentOtherJmxStats = otherAttributeToJmxStats.get(attributeKey);
			currentThisJmxStats.add(currentOtherJmxStats);
		}
	}

	@Override
	public void refreshStats(long timestamp, double smoothingWindow) {
		for (JmxStats<?> jmxStats : fetchNameToJmxStats(this).values()) {
			jmxStats.refreshStats(timestamp, smoothingWindow);
		}
	}

	@Override
	public SortedMap<String, TypeAndValue> getAttributes() {
		SortedMap<String, TypeAndValue> attributeToTypeAndValue = new TreeMap<>();
		SortedMap<String, JmxStats<?>> nameToJmxStats = fetchNameToJmxStats(this);
		for (String currentJmxStatsName : nameToJmxStats.keySet()) {
			JmxStats<?> currentJmxStats = nameToJmxStats.get(currentJmxStatsName);
			Map<String, TypeAndValue> currentJmxStatsInnerAttributes = currentJmxStats.getAttributes();
			for (String oldKey : currentJmxStatsInnerAttributes.keySet()) {
				String newKey = format(ATTRIBUTE_NAME_PATTERN, currentJmxStatsName, oldKey);
				attributeToTypeAndValue.put(newKey, currentJmxStatsInnerAttributes.get(oldKey));
			}
		}
		return attributeToTypeAndValue;
	}
}
