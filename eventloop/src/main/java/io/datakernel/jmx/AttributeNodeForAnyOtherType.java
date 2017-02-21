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
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;

final class AttributeNodeForAnyOtherType extends AttributeNodeForLeafAbstract {

	public AttributeNodeForAnyOtherType(String name, String description, boolean visible, ValueFetcher fetcher) {
		super(name, description, fetcher, visible);
	}

	@Override
	public Map<String, OpenType<?>> getOpenTypes() {
		return Collections.<String, OpenType<?>>singletonMap(name, SimpleType.STRING);
	}

	@Override
	public Object aggregateAttribute(String attrName, List<?> sources) {
		Object firstPojo = sources.get(0);
		Object firstValue = (fetcher.fetchFrom(firstPojo));
		if (firstValue == null) {
			return null;
		}

		for (int i = 1; i < sources.size(); i++) {
			Object currentPojo = sources.get(i);
			Object currentValue = Objects.toString(fetcher.fetchFrom(currentPojo));
			if (!Objects.equals(firstPojo, currentValue)) {
				return null;
			}
		}
		return firstValue.toString();
	}

	@Override
	public boolean isSettable(String attrName) {
		assert name.equals(attrName);

		return false;
	}

	@Override
	public void setAttribute(String attrName, Object value, List<?> targets) {
		throw new UnsupportedOperationException();
	}

	@Override
	public final Iterable<JmxRefreshable> getAllRefreshables(Object source) {
		return null;
	}
}
