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

import static io.datakernel.util.Preconditions.checkArgument;

public final class MBeanSettings {
	private final List<String> includedOptionals = new ArrayList<>();
	private final Map<String, AttributeModifier<?>> modifiers = new HashMap<>();

	private MBeanSettings(List<String> includedOptionals, Map<String, ? extends AttributeModifier<?>> modifiers) {
		this.includedOptionals.addAll(includedOptionals);
		this.modifiers.putAll(modifiers);
	}

	public static MBeanSettings of(List<String> includedOptionals,
	                               Map<String, ? extends AttributeModifier<?>> modifiers) {
		return new MBeanSettings(includedOptionals, modifiers);
	}

	public static MBeanSettings defaultSettings() {
		return new MBeanSettings(new ArrayList<String>(), new HashMap<String, AttributeModifier<?>>());
	}

	public void addIncludedOptional(String attrName) {
		includedOptionals.add(attrName);
	}

	public void addModifier(String attrName, AttributeModifier<?> modifier) {
		checkArgument(!modifiers.containsKey(attrName), "cannot add two modifiers for one attribute");

		modifiers.put(attrName, modifier);
	}

	public List<String> getIncludedOptionals() {
		return includedOptionals;
	}

	public Map<String, ? extends AttributeModifier<?>> getModifiers() {
		return modifiers;
	}
}
