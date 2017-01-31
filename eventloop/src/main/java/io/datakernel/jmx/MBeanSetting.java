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

import java.util.Collections;
import java.util.List;
import java.util.Map;

public final class MBeanSetting {
	private final List<String> includedOptionals;
	private final Map<String, AttributeModifier<?>> modifiers;

	private MBeanSetting(List<String> includedOptionals, Map<String, AttributeModifier<?>> modifiers) {
		this.includedOptionals = includedOptionals;
		this.modifiers = modifiers;
	}

	public static MBeanSetting of(List<String> includedOptionals, Map<String, AttributeModifier<?>> modifiers) {
		return new MBeanSetting(includedOptionals, modifiers);
	}

	public static MBeanSetting defaultSettings() {
		return new MBeanSetting(Collections.<String>emptyList(), Collections.<String, AttributeModifier<?>>emptyMap());
	}

	public List<String> getIncludedOptionals() {
		return includedOptionals;
	}

	public Map<String, AttributeModifier<?>> getModifiers() {
		return modifiers;
	}
}
