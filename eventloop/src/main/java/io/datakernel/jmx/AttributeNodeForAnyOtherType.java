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

import javax.management.openmbean.OpenType;
import javax.management.openmbean.SimpleType;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import static io.datakernel.jmx.Utils.createMapWithOneEntry;
import static io.datakernel.jmx.Utils.filterNulls;
import static io.datakernel.util.Preconditions.checkArgument;
import static io.datakernel.util.Preconditions.checkNotNull;

final class AttributeNodeForAnyOtherType implements AttributeNode {
	private final String name;
	private final ValueFetcher fetcher;
	private final OpenType<?> openType;
	private final Map<String, OpenType<?>> nameToOpenType;

	public AttributeNodeForAnyOtherType(String name, ValueFetcher fetcher) {
		checkArgument(!name.isEmpty(), "Leaf attribute cannot have empty name");

		this.name = name;
		this.fetcher = fetcher;
		this.openType = SimpleType.STRING;
		this.nameToOpenType = createMapWithOneEntry(name, openType);
	}

	@Override
	public String getName() {
		return name;
	}

	@Override
	public OpenType<?> getOpenType() {
		return openType;
	}

	@Override
	public Map<String, OpenType<?>> getFlattenedOpenTypes() {
		return nameToOpenType;
	}

	@Override
	public Map<String, Object> aggregateAllAttributes(List<?> sources) {
		Map<String, Object> attrs = new HashMap<>();
		attrs.put(name, aggregateAttribute(name, sources));
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

		Object firstPojo = notNullSources.get(0);
		Object firstValueStringRepresentation = Objects.toString(fetcher.fetchFrom(firstPojo));
		for (int i = 1; i < notNullSources.size(); i++) {
			Object currentPojo = notNullSources.get(i);
			Object currentValueStringRepresentation = Objects.toString(fetcher.fetchFrom(currentPojo));
			if (!Objects.equals(firstValueStringRepresentation, currentValueStringRepresentation)) {
				return null;
			}
		}
		return firstValueStringRepresentation.toString();
	}

	@Override
	public Iterable<JmxRefreshable> getAllRefreshables(Object source) {
		return null;
	}

	@Override
	public boolean isSettable(String attrName) {
		checkArgument(attrName.equals(name));

		return false;
	}

	@Override
	public void setAttribute(String attrName, Object value, List<?> targets) {
		throw new UnsupportedOperationException();
	}

}
