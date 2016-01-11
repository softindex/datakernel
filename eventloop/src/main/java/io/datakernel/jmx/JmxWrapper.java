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

import io.datakernel.jmx.annotation.JmxNamedParameter;
import io.datakernel.jmx.annotation.JmxOperation;

import javax.management.*;
import java.lang.annotation.Annotation;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static io.datakernel.jmx.Utils.fetchNameToJmxStats;
import static io.datakernel.jmx.Utils.fetchNameToJmxStatsGetter;
import static io.datakernel.util.Preconditions.checkArgument;
import static io.datakernel.util.Preconditions.checkNotNull;
import static java.lang.String.format;

public class JmxWrapper implements DynamicMBean {
	private static final String ATTRIBUTE_NAME_FORMAT = "%s_%s";
	private static final String ATTRIBUTE_NAME_REGEX = "^([a-zA-Z0-9]+)_(\\w+)$";
	private static final Pattern ATTRIBUTE_NAME_PATTERN = Pattern.compile(ATTRIBUTE_NAME_REGEX);
	private static final String ATTRIBUTE_DEFAULT_DESCRIPTION = "";

	private final MBeanInfo monitorableMBeanInfo;
	private final JmxMonitorable monitorable;

	private Map<String, Method> attributeGetters;
	private Map<OperationKey, Method> operationKeyToMethod = new HashMap<>();

	private JmxWrapper(JmxMonitorable monitorable) {
		this.monitorable = monitorable;
		String monitorableName = "";
		String monitorableDescription = "";
		MBeanAttributeInfo[] attributes = extractAttributesInfo(monitorable);

		this.attributeGetters = fetchNameToJmxStatsGetter(monitorable);

		MBeanConstructorInfo[] constructors = null;
		MBeanOperationInfo[] operations = extractOperationsInfo(monitorable);
		MBeanNotificationInfo[] notifications = null;
		this.monitorableMBeanInfo = new MBeanInfo(monitorableName, monitorableDescription,
				attributes, constructors, operations, notifications);
	}

	private static MBeanAttributeInfo[] extractAttributesInfo(JmxMonitorable monitorable) {
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
		return attributes.toArray(new MBeanAttributeInfo[attributes.size()]);
	}

	private MBeanOperationInfo[] extractOperationsInfo(JmxMonitorable monitorable) {
		// TODO(vmykhalko): refactor this method
		List<MBeanOperationInfo> operations = new ArrayList<>();
		Method[] methods = monitorable.getClass().getMethods();
		for (Method method : methods) {
			if (method.isAnnotationPresent(JmxOperation.class)) {
				JmxOperation annotation = method.getAnnotation(JmxOperation.class);
				String opName = annotation.name();
				if (opName.equals("")) {
					opName = method.getName();
				}
				String opDescription = annotation.description();
				Class<?> returnType = method.getReturnType();
				List<MBeanParameterInfo> params = new ArrayList<>();
				Class<?>[] paramTypes = method.getParameterTypes();
				Annotation[][] paramAnnotations = method.getParameterAnnotations();

				assert paramAnnotations.length == paramTypes.length;

				for (int i = 0; i < paramTypes.length; i++) {
					String paramName = String.format("arg%d", i);
					Class<?> paramType = paramTypes[i];
					JmxNamedParameter nameAnnotation = findJmxNamedParameterAnnotation(paramAnnotations[i]);
					if (nameAnnotation != null) {
						paramName = nameAnnotation.value();
					}
					MBeanParameterInfo paramInfo = new MBeanParameterInfo(paramName, paramType.getName(), "");
					params.add(paramInfo);
				}
				MBeanParameterInfo[] paramsArray = params.toArray(new MBeanParameterInfo[params.size()]);
				MBeanOperationInfo operationInfo = new MBeanOperationInfo(
						opName, opDescription, paramsArray, returnType.getName(), MBeanOperationInfo.ACTION);
				operations.add(operationInfo);

				// TODO(vmykhalko): refactor to remove this side-effect, make method static
				String[] paramTypesNames = new String[paramTypes.length];
				for (int i = 0; i < paramTypes.length; i++) {
					paramTypesNames[i] = paramTypes[i].getName();
				}
				operationKeyToMethod.put(new OperationKey(opName, paramTypesNames), method);

			}
		}
		return operations.toArray(new MBeanOperationInfo[operations.size()]);
	}

	private static JmxNamedParameter findJmxNamedParameterAnnotation(Annotation[] annotations) {
		for (Annotation annotation : annotations) {
			if (annotation.annotationType().equals(JmxNamedParameter.class)) {
				return (JmxNamedParameter) annotation;
			}
		}
		return null;
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
				jmxStats = (JmxStats<?>) jmxStatsGetter.invoke(monitorable);
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
	public void setAttribute(Attribute attribute)
			throws AttributeNotFoundException, InvalidAttributeValueException, MBeanException, ReflectionException {
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
	public Object invoke(String actionName, Object[] params, String[] signature)
			throws MBeanException, ReflectionException {
		String[] argTypes = signature != null ? signature : new String[0];
		OperationKey opkey = new OperationKey(actionName, argTypes);
		Method opMethod = operationKeyToMethod.get(opkey);
		if (opMethod == null) {
			// TODO(vmykhalko): maybe throw another exception
			String errorMsg =
					"There is no operation with name: " + actionName + " and signature: " + Arrays.toString(signature);
			throw new RuntimeOperationsException(new IllegalArgumentException("Operation not found"), errorMsg);
		}
		try {
			Object[] args = params != null ? params : new Object[0];
			return opMethod.invoke(monitorable, args);
		} catch (IllegalAccessException | InvocationTargetException e) {
			throw new ReflectionException(e);
		}
	}

	@Override
	public MBeanInfo getMBeanInfo() {
		return monitorableMBeanInfo;
	}

	private static final class OperationKey {
		private final String name;
		private final String[] argTypes;

		public OperationKey(String name, String[] argTypes) {
			checkNotNull(name);
			checkNotNull(argTypes);

			this.name = name;
			this.argTypes = argTypes;
		}

		public String getName() {
			return name;
		}

		public String[] getArgTypes() {
			return argTypes;
		}

		@Override
		public boolean equals(Object o) {
			if (this == o) return true;
			if (!(o instanceof OperationKey)) return false;

			OperationKey that = (OperationKey) o;

			if (!name.equals(that.name)) return false;
			// TODO(vmykhalko): check this autogenerated comment
			// Probably incorrect - comparing Object[] arrays with Arrays.equals
			return Arrays.equals(argTypes, that.argTypes);

		}

		@Override
		public int hashCode() {
			int result = name.hashCode();
			result = 31 * result + Arrays.hashCode(argTypes);
			return result;
		}
	}
}
