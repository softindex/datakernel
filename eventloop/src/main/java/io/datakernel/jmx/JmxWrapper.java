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
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.SortedMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static io.datakernel.jmx.Utils.fetchNameToJmxStats;
import static io.datakernel.jmx.Utils.fetchNameToJmxStatsGetter;
import static io.datakernel.util.Preconditions.checkArgument;
import static java.lang.String.format;

public class JmxWrapper implements DynamicMBean {
	private static final String ATTRIBUTE_NAME_FORMAT = "%s_%s";
	private static final String ATTRIBUTE_NAME_REGEX = "^([a-zA-Z0-9]+)_(\\w+)$";
	private static final Pattern ATTRIBUTE_NAME_PATTERN = Pattern.compile(ATTRIBUTE_NAME_REGEX);
	private static final String ATTRIBUTE_DEFAULT_DESCRIPTION = "";

	private final MBeanInfo monitorableMBeanInfo;
	private final JmxMonitorable monitorable;

	private Map<String, Method> attributeGetters;

	private JmxWrapper(JmxMonitorable monitorable) {
		this.monitorable = monitorable;
		String monitorableName = "";
		String monitorableDescription = "";
		List<MBeanAttributeInfo> attributes = extractAttributesInfo(monitorable);

		this.attributeGetters = fetchNameToJmxStatsGetter(monitorable);

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
				String attrName = format(ATTRIBUTE_NAME_FORMAT, statsName, statsAttributeName);
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
		// TODO(vmykhalko): try to refactor this method in future
		Matcher matcher = ATTRIBUTE_NAME_PATTERN.matcher(attribute);
		if (matcher.matches()) {
			String jmxStatsName = matcher.group(1);
			Method jmxStatsGetter = attributeGetters.get(jmxStatsName);
			if (jmxStatsGetter == null) {
				throw new AttributeNotFoundException();
			}
			JmxStats<?> jmxStats = null;
			try {
				jmxStats = (JmxStats<?>)jmxStatsGetter.invoke(monitorable);
			} catch (IllegalAccessException | InvocationTargetException e) {
				throw new ReflectionException(e);
			}
			String attrNameRest = matcher.group(2);
			Map<String, JmxStats.TypeAndValue> jmxStatsAttributes = jmxStats.getAttributes();
			JmxStats.TypeAndValue typeAndValue = jmxStatsAttributes.get(attrNameRest);
			if (typeAndValue == null) {
				throw new AttributeNotFoundException();
			}
			return typeAndValue.getValue();
		} else {
			throw new AttributeNotFoundException();
		}
	}

	@Override
	public void setAttribute(Attribute attribute) throws AttributeNotFoundException, InvalidAttributeValueException, MBeanException, ReflectionException {
		throw new UnsupportedOperationException("Setting of attributes is not available");
	}

	@Override
	public AttributeList getAttributes(String[] attributes) {
		checkArgument(attributes != null);

		AttributeList attrList = new AttributeList();
		for (String attrName : attributes) {
			try {
				attrList.add(new Attribute(attrName, getAttribute(attrName)));
			} catch (AttributeNotFoundException | MBeanException | ReflectionException e) {
				e.printStackTrace();
			}
		}
		return attrList;
	}

	@Override
	public AttributeList setAttributes(AttributeList attributes) {
		throw new UnsupportedOperationException("Setting of attributes is not available");
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
