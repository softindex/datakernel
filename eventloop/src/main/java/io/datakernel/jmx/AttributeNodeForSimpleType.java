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
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.*;

import static io.datakernel.jmx.Utils.*;
import static io.datakernel.util.Preconditions.checkArgument;
import static io.datakernel.util.Preconditions.checkNotNull;

final class AttributeNodeForSimpleType implements AttributeNode {
	private final String name;
	private final String description;
	private final ValueFetcher fetcher;
	private final Method setter;
	private final Class<?> attributeType;
	private final OpenType<?> openType;
	private final Map<String, OpenType<?>> nameToOpenType;
	private final JmxReducer reducer;
	private final boolean visible;

	public AttributeNodeForSimpleType(String name, String description, boolean visible,
	                                  ValueFetcher fetcher, Method setter,
	                                  Class<?> attributeType, JmxReducer reducer) {
		this.name = name;
		this.description = description;
		this.fetcher = fetcher;
		this.setter = setter;
		this.attributeType = attributeType;
		this.openType = simpleTypeOf(attributeType);
		this.nameToOpenType = wrapAttributeInMap(name, openType, visible);
		this.reducer = checkNotNull(reducer);
		this.visible = visible;
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
		return openType;
	}

	@Override
	public Map<String, OpenType<?>> getVisibleFlattenedOpenTypes() {
		return nameToOpenType;
	}

	@Override
	public Set<String> getAllFlattenedAttrNames() {
		return Collections.singleton(name);
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

		List<Object> values = new ArrayList<>(notNullSources.size());
		for (Object notNullSource : notNullSources) {
			Object currentValue = fetcher.fetchFrom(notNullSource);
			values.add(currentValue);
		}

		return reducer.reduce(values);
	}

	@Override
	public Iterable<JmxRefreshable> getAllRefreshables(Object source) {
		return null;
	}

	@Override
	public boolean isSettable(String attrName) {
		checkArgument(attrName.equals(name));

		return setter != null;
	}

	@Override
	public void setAttribute(String attrName, Object value, List<?> targets) throws SetterException {
		checkArgument(attrName.equals(name));
		checkNotNull(targets);
		List<?> notNullTargets = filterNulls(targets);
		if (notNullTargets.size() == 0) {
			return;
		}

		for (Object target : notNullTargets) {
			try {
				setter.invoke(target, value);
			} catch (IllegalAccessException | InvocationTargetException e) {
				throw new SetterException(e);
			}
		}
	}

	@Override
	public AttributeNode rebuildOmittingNullPojos(List<?> sources) {
		return this;
	}

	@Override
	public boolean isVisible() {
		return visible;
	}

	@Override
	public AttributeNode rebuildWithVisible(String attrName) {
		return new AttributeNodeForSimpleType(name, description, true, fetcher, setter, attributeType, reducer);
	}

	@Override
	public void applyModifier(String attrName, AttributeModifier<?> modifier, List<?> target) {
		throw new UnsupportedOperationException();
	}

	private static SimpleType<?> simpleTypeOf(Class<?> clazz) throws IllegalArgumentException {
		if (clazz == boolean.class || clazz == Boolean.class) {
			return SimpleType.BOOLEAN;
		} else if (clazz == byte.class || clazz == Byte.class) {
			return SimpleType.BYTE;
		} else if (clazz == short.class || clazz == Short.class) {
			return SimpleType.SHORT;
		} else if (clazz == char.class || clazz == Character.class) {
			return SimpleType.CHARACTER;
		} else if (clazz == int.class || clazz == Integer.class) {
			return SimpleType.INTEGER;
		} else if (clazz == long.class || clazz == Long.class) {
			return SimpleType.LONG;
		} else if (clazz == float.class || clazz == Float.class) {
			return SimpleType.FLOAT;
		} else if (clazz == double.class || clazz == Double.class) {
			return SimpleType.DOUBLE;
		} else if (clazz == String.class) {
			return SimpleType.STRING;
		} else {
			throw new IllegalArgumentException("There is no SimpleType for " + clazz.getName());
		}
	}
}
