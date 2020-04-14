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
import io.datakernel.jmx.api.ConcurrentJmxMBean;
import io.datakernel.jmx.api.JmxAttribute;
import io.datakernel.jmx.stats.JmxStats;
import org.junit.Test;

import javax.management.DynamicMBean;
import javax.management.MBeanAttributeInfo;
import javax.management.MBeanInfo;
import java.lang.reflect.Type;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import static io.datakernel.jmx.helper.Utils.nameToAttribute;
import static java.util.Arrays.asList;
import static java.util.Collections.singleton;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class DynamicMBeanFactoryImplSettingsTest {
	private static final Set<String> NO_MONITORABLES = Collections.emptySet();
	private static final Map<String, AttributeModifier<?>> NO_MODIFIERS = Collections.emptyMap();
	private static final Map<Type, JmxCustomTypeAdapter<?>> NO_CUSTOM_TYPES = Collections.emptyMap();

	// region included optionals
	@Test
	public void includesOptionalAttributes_thatAreSpecifiedInSettings() {
		MBeanSettings settings = MBeanSettings.of(singleton("stats_text"), NO_MODIFIERS, NO_CUSTOM_TYPES);
		DynamicMBean mbean = DynamicMBeanFactoryImpl.create()
				.createDynamicMBean(asList(new MBeanStubOne()), settings, false);

		MBeanInfo mBeanInfo = mbean.getMBeanInfo();
		Map<String, MBeanAttributeInfo> attrs = nameToAttribute(mBeanInfo.getAttributes());

		assertEquals(2, attrs.size());
		assertTrue(attrs.containsKey("stats_text"));
	}

	public static final class MBeanStubOne implements ConcurrentJmxMBean {
		private final JmxStatsStub stats = new JmxStatsStub();

		@JmxAttribute
		public JmxStatsStub getStats() {
			return stats;
		}
	}

	public static final class JmxStatsStub implements JmxStats<JmxStatsStub> {

		@Override
		public void add(JmxStatsStub another) {

		}

		@JmxAttribute
		public int getNumber() {
			return 10;
		}

		@JmxAttribute(optional = true)
		public String getText() {
			return "text";
		}

		@JmxAttribute(optional = true)
		public String getData() {
			return "data";
		}
	}
	// endregion

	// region modifiers
	@Test
	public void modifiesDynamicMBeanComponentsAccordingToSettings() throws Exception {
		Map<String, AttributeModifier<?>> nameToModifier = new HashMap<>();
		nameToModifier.put("stats", (AttributeModifier<ConfigurableStats>) attribute -> attribute.setConfigurableText("configurated"));
		MBeanSettings settings = MBeanSettings.of(NO_MONITORABLES, nameToModifier, NO_CUSTOM_TYPES);
		MBeanStubTwo mBeanStubTwo = new MBeanStubTwo();
		DynamicMBean mbean = DynamicMBeanFactoryImpl.create()
				.createDynamicMBean(asList(mBeanStubTwo), settings, false);

		assertEquals("configurated", mbean.getAttribute("stats_data"));
	}

	private static final class MBeanStubTwo implements ConcurrentJmxMBean {
		private final ConfigurableStats stats = new ConfigurableStats();

		@JmxAttribute
		public ConfigurableStats getStats() {
			return stats;
		}
	}

	public static final class ConfigurableStats implements JmxStats<ConfigurableStats> {
		private String configurableText = "";

		@JmxAttribute
		public String getData() {
			return configurableText;
		}

		public void setConfigurableText(String text) {
			configurableText = text;
		}

		@Override
		public void add(ConfigurableStats another) {
			this.configurableText += another.configurableText;
		}
	}

	// endregion
}
