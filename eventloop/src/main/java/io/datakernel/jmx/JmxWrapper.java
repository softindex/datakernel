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

import javax.management.*;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.SortedMap;

import static io.datakernel.jmx.Utils.fetchNameToJmxStats;
import static java.lang.String.format;

public class JmxWrapper implements DynamicMBean {

	private static final String ATTRIBUTE_NAME_PATTERN = "%s_%s";
	private static final String ATTRIBUTE_DEFAULT_DESCRIPTION = "";

	private final MBeanInfo monitorableMBeanInfo;
	private final JmxMonitorable monitorable;

	private JmxWrapper(JmxMonitorable monitorable) {
		this.monitorable = monitorable;
		String monitorableName = "";
		String monitorableDescription = "";
		List<MBeanAttributeInfo> attributes = extractAttributesInfo(monitorable);

		MBeanConstructorInfo[] constructors = null;
		MBeanOperationInfo[] operations = null;
		MBeanNotificationInfo[] notifications = null;
		this.monitorableMBeanInfo = new MBeanInfo(monitorableName, monitorableDescription,
				attributes.toArray(new MBeanAttributeInfo[attributes.size()]),
				constructors, operations, notifications);
	}

	private static List<MBeanAttributeInfo> extractAttributesInfo(JmxMonitorable monitorable) {
		List<MBeanAttributeInfo> attributes = new ArrayList<>();
		Map<String, JmxStats<?>> nameToJmxStats = fetchNameToJmxStats(monitorable);
		for (String statsName : nameToJmxStats.keySet()) {
			JmxStats<?> stats = nameToJmxStats.get(statsName);
			SortedMap<String, JmxStats.TypeAndValue> statsAttributes = stats.getAttributes();
			for (String statsAttributeName : statsAttributes.keySet()) {
				JmxStats.TypeAndValue typeAndValue = statsAttributes.get(statsAttributeName);
				String attrName = format(ATTRIBUTE_NAME_PATTERN, statsName, statsAttributeName);
				String attrType = typeAndValue.getType().getTypeName();
				MBeanAttributeInfo attributeInfo =
						new MBeanAttributeInfo(attrName, attrType, ATTRIBUTE_DEFAULT_DESCRIPTION, true, false, false);
				attributes.add(attributeInfo);
			}
		}
		return attributes;
	}

	public static JmxWrapper wrap(JmxMonitorable monitorable) {
		return new JmxWrapper(monitorable);
	}

	@Override
	public Object getAttribute(String attribute) throws AttributeNotFoundException, MBeanException, ReflectionException {
		return null;
	}

	@Override
	public void setAttribute(Attribute attribute) throws AttributeNotFoundException, InvalidAttributeValueException, MBeanException, ReflectionException {

	}

	@Override
	public AttributeList getAttributes(String[] attributes) {
		return null;
	}

	@Override
	public AttributeList setAttributes(AttributeList attributes) {
		return null;
	}

	@Override
	public Object invoke(String actionName, Object[] params, String[] signature) throws MBeanException, ReflectionException {
		return null;
	}

	@Override
	public MBeanInfo getMBeanInfo() {
		return monitorableMBeanInfo;
	}
}
