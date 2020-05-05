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

import io.datakernel.jmx.api.ConcurrentJmxBean;
import io.datakernel.jmx.api.attribute.JmxAttribute;
import org.junit.Test;

import javax.management.DynamicMBean;
import javax.management.MBeanAttributeInfo;
import java.util.Map;

import static io.datakernel.jmx.JmxBeanSettings.defaultSettings;
import static io.datakernel.jmx.helper.Utils.nameToAttribute;
import static java.util.Arrays.asList;
import static junit.framework.TestCase.assertEquals;

public class DynamicMBeanFactoryAttributeDescriptionTest {

	@Test
	public void ifDescriptionIsNotSpecifiedItIsSameAsFullNameOfAttribute() {
		DynamicMBean mbean = DynamicMBeanFactory.create()
				.createDynamicMBean(asList(new MBeanWithNoJmxDescription()), defaultSettings(), false);

		Map<String, MBeanAttributeInfo> nameToAttr = nameToAttribute(mbean.getMBeanInfo().getAttributes());

		assertEquals("stats_count", nameToAttr.get("stats_count").getDescription());
	}

	public static final class MBeanWithNoJmxDescription implements ConcurrentJmxBean {
		@JmxAttribute
		public SimplePojo getStats() {
			return new SimplePojo();
		}

		public static final class SimplePojo {
			@JmxAttribute
			public int getCount() {
				return 0;
			}
		}
	}

	@Test
	public void showsDescriptionWithoutChangesIfAttributeNameDoNotContainUnderscores() {
		DynamicMBean mbean = DynamicMBeanFactory.create()
				.createDynamicMBean(asList(new MBeanWithDescriptionInDirectNonPojoAttribute()), defaultSettings(), false);

		Map<String, MBeanAttributeInfo> nameToAttr = nameToAttribute(mbean.getMBeanInfo().getAttributes());

		assertEquals("description of count", nameToAttr.get("count").getDescription());
		assertEquals("description of inner count", nameToAttr.get("innerCount").getDescription());
	}

	public static final class MBeanWithDescriptionInDirectNonPojoAttribute implements ConcurrentJmxBean {
		@JmxAttribute(description = "description of count")
		public int getCount() {
			return 0;
		}

		@JmxAttribute(name = "")
		public Group getGroup() {
			return new Group();
		}

		public static final class Group {
			@JmxAttribute(description = "description of inner count")
			public int getInnerCount() {
				return 0;
			}
		}
	}

	@Test
	public void formatsDescriptionsProperlyIfAttributeNameContainsUnderscores() {
		DynamicMBean mbean = DynamicMBeanFactory.create()
				.createDynamicMBean(asList(new MBeanWithPojoDescription()), defaultSettings(), false);

		Map<String, MBeanAttributeInfo> nameToAttr = nameToAttribute(mbean.getMBeanInfo().getAttributes());

		assertEquals("\"stats\": desc of first-level pojo  |  \"info\": desc of info",
				nameToAttr.get("stats_innerStats_info").getDescription());
	}

	public static final class MBeanWithPojoDescription implements ConcurrentJmxBean {
		@JmxAttribute(description = "desc of first-level pojo")
		public Stats getStats() {
			return new Stats();
		}

		public static final class Stats {
			@JmxAttribute // no description here
			public InnerStats getInnerStats() {
				return new InnerStats();
			}

			public static final class InnerStats {
				@JmxAttribute(description = "desc of info")
				public String getInfo() {
					return "";
				}
			}
		}
	}
}
