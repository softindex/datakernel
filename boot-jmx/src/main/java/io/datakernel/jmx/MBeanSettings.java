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

import io.datakernel.jmx.DynamicMBeanFactoryImpl.JmxCustomTypeAdapter;

import java.lang.reflect.Type;
import java.util.*;

import static io.datakernel.common.Preconditions.checkArgument;

public final class MBeanSettings {
	private final Set<String> includedOptionals = new HashSet<>();
	private final Map<String, AttributeModifier<?>> modifiers = new HashMap<>();
	private final Map<Type, JmxCustomTypeAdapter<?>> customTypes = new HashMap<>();

	private MBeanSettings(Set<String> includedOptionals, Map<String, ? extends AttributeModifier<?>> modifiers, Map<Type, JmxCustomTypeAdapter<?>> customTypes) {
		this.includedOptionals.addAll(includedOptionals);
		this.modifiers.putAll(modifiers);
		this.customTypes.putAll(customTypes);
	}

	public static MBeanSettings of(Set<String> includedOptionals,
			Map<String, ? extends AttributeModifier<?>> modifiers,
			Map<Type, JmxCustomTypeAdapter<?>> customTypes) {
		return new MBeanSettings(includedOptionals, modifiers, customTypes);
	}

	public static MBeanSettings create() {
		return new MBeanSettings(new HashSet<>(), new HashMap<>(), new HashMap<>());
	}

	public static MBeanSettings defaultSettings() {
		return new MBeanSettings(Collections.emptySet(), Collections.emptyMap(), Collections.emptyMap());
	}

	public void merge(MBeanSettings otherSettings) {
		includedOptionals.addAll(otherSettings.includedOptionals);
		modifiers.putAll(otherSettings.modifiers);
		customTypes.putAll(otherSettings.customTypes);
	}

	public MBeanSettings withIncludedOptional(String attrName) {
		includedOptionals.add(attrName);
		return this;
	}

	public MBeanSettings withModifier(String attrName, AttributeModifier<?> modifier) {
		checkArgument(!modifiers.containsKey(attrName), "cannot add two modifiers for one attribute");
		modifiers.put(attrName, modifier);
		return this;
	}

	public MBeanSettings withCustomTypes(Map<Type, JmxCustomTypeAdapter<?>> customTypes) {
		this.customTypes.putAll(customTypes);
		return this;
	}

	public Set<String> getIncludedOptionals() {
		return includedOptionals;
	}

	public Map<String, ? extends AttributeModifier<?>> getModifiers() {
		return modifiers;
	}

	public Map<Type, JmxCustomTypeAdapter<?>> getCustomTypes() {
		return customTypes;
	}
}
