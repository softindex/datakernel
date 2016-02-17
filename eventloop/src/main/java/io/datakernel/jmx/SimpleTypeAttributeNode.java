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

import javax.management.openmbean.SimpleType;
import java.lang.reflect.Method;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

final class SimpleTypeAttributeNode extends AbstractAttributeNode {

	public SimpleTypeAttributeNode(String name, Method getter, SimpleType<?> simpleType) {
		super(name, getter, simpleType);
	}

	@Override
	public Map<String, Object> aggregateAllAttributes(List<?> pojos) {
		Map<String, Object> attrs = new HashMap<>();
		attrs.put(getName(), aggregateAttribute(pojos, null));
		return attrs;
	}

	@Override
	public Object aggregateAttribute(List<?> pojos, String attrName) {
		checkPojos(pojos);

		// we ignore attrName here because this is leaf-node

		Object firstPojo = pojos.get(0);
		Object firstValue = fetchValueFrom(firstPojo);
		for (int i = 1; i < pojos.size(); i++) {
			Object currentPojo = pojos.get(i);
			Object currentValue = fetchValueFrom(currentPojo);
			if (!firstValue.equals(currentValue)) {
				// TODO (vmykhalko): maybe throw AggregationException instead ?
				return null;
			}
		}
		return firstValue;
	}
}
