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

import io.datakernel.jmx.annotation.JmxMBean;
import io.datakernel.jmx.annotation.JmxOperation;
import io.datakernel.jmx.annotation.JmxParameter;

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
import static java.util.Arrays.asList;

public final class DynamicMBeanFactory {
	private static final String ATTRIBUTE_NAME_FORMAT = "%s_%s";
	private static final String ATTRIBUTE_NAME_REGEX = "^([a-zA-Z0-9]+)_(\\w+)$";
	private static final Pattern ATTRIBUTE_NAME_PATTERN = Pattern.compile(ATTRIBUTE_NAME_REGEX);
	private static final String ATTRIBUTE_DEFAULT_DESCRIPTION = "";

	private DynamicMBeanFactory() {

	}

	public static DynamicMBean createFor(Object... monitorables) throws Exception {
		checkNotNull(monitorables);
		checkArgument(monitorables.length > 0);
		checkArgument(!arrayContainsNullValues(monitorables), "monitorable can not be null");
		checkArgument(allObjectsAreOfSameType(asList(monitorables)));
		checkArgument(allObjectsAreAnnotatedWithJmxMBean(asList(monitorables)));

		// all objects are of same type, so we can extract info from any of them
		Object first = monitorables[0];
		MBeanInfo mBeanInfo = composeMBeanInfo(first);
		Map<String, Class<? extends JmxStats<?>>> nameToJmxStatsType = fetchNameToJmxStatsType(first);

		List<JmxMonitorableWrapper> wrappers = new ArrayList<>();
		for (Object monitorable : monitorables) {
			wrappers.add(createWrapper(monitorable));
		}

		return new DynamicMBeanAggregator(mBeanInfo, wrappers, nameToJmxStatsType);
	}

	private static MBeanAttributeInfo[] extractAttributesInfo(Object monitorable) {
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

	private static MBeanOperationInfo[] extractOperationsInfo(Object monitorable) {
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
					JmxParameter nameAnnotation = findJmxNamedParameterAnnotation(paramAnnotations[i]);
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
			}
		}
		return operations.toArray(new MBeanOperationInfo[operations.size()]);
	}

	private static JmxParameter findJmxNamedParameterAnnotation(Annotation[] annotations) {
		for (Annotation annotation : annotations) {
			if (annotation.annotationType().equals(JmxParameter.class)) {
				return (JmxParameter) annotation;
			}
		}
		return null;
	}

	private static <T> boolean arrayContainsNullValues(T[] array) {
		for (int i = 0; i < array.length; i++) {
			if (array[i] == null) {
				return true;
			}
		}
		return false;
	}

	private static boolean allObjectsAreOfSameType(List<?> objects) {
		for (int i = 0; i < objects.size() - 1; i++) {
			Object current = objects.get(i);
			Object next = objects.get(i + 1);
			if (!current.getClass().equals(next.getClass())) {
				return false;
			}
		}
		return true;
	}

	private static boolean allObjectsAreAnnotatedWithJmxMBean(List<?> objects) {
		for (Object object : objects) {
			if (!object.getClass().isAnnotationPresent(JmxMBean.class)) {
				return false;
			}
		}
		return true;
	}

	private static MBeanInfo composeMBeanInfo(Object monitorable) {
		String monitorableName = "";
		String monitorableDescription = "";
		MBeanAttributeInfo[] attributes = extractAttributesInfo(monitorable);
		MBeanOperationInfo[] operations = extractOperationsInfo(monitorable);
		return new MBeanInfo(
				monitorableName,
				monitorableDescription,
				attributes,
				null,  // constructors
				operations,
				null); //notifications
	}

	@SuppressWarnings("unchecked")
	private static Map<String, Class<? extends JmxStats<?>>> fetchNameToJmxStatsType(Object monitorable) throws InvocationTargetException, IllegalAccessException {
		Map<String, Method> nameToGetter = fetchNameToJmxStatsGetter(monitorable);
		Map<String, Class<? extends JmxStats<?>>> nameToJmxStatsType = new HashMap<>();
		for (String name : nameToGetter.keySet()) {
			Method jmxStatsGetter = nameToGetter.get(name);
			JmxStats jmxStats = (JmxStats<?>) jmxStatsGetter.invoke(monitorable);
			nameToJmxStatsType.put(name, (Class<? extends JmxStats<?>>) jmxStats.getClass());
		}
		return nameToJmxStatsType;
	}

	private static JmxMonitorableWrapper createWrapper(Object monitorable) {
		Map<String, Method> attributeGetters = fetchNameToJmxStatsGetter(monitorable);
		Map<OperationKey, Method> opkeyToMethod = fetchOpkeyToMethod(monitorable);
		return new JmxMonitorableWrapper(monitorable, attributeGetters, opkeyToMethod);
	}

	// TODO(vmykhalko): refactor this method (it has common code with  extractOperationsInfo()
	private static Map<OperationKey, Method> fetchOpkeyToMethod(Object monitorable) {
		Map<OperationKey, Method> opkeyToMethod = new HashMap<>();
		Method[] methods = monitorable.getClass().getMethods();
		for (Method method : methods) {
			if (method.isAnnotationPresent(JmxOperation.class)) {
				JmxOperation annotation = method.getAnnotation(JmxOperation.class);
				String opName = annotation.name();
				if (opName.equals("")) {
					opName = method.getName();
				}
				Class<?>[] paramTypes = method.getParameterTypes();
				Annotation[][] paramAnnotations = method.getParameterAnnotations();

				assert paramAnnotations.length == paramTypes.length;

				String[] paramTypesNames = new String[paramTypes.length];
				for (int i = 0; i < paramTypes.length; i++) {
					paramTypesNames[i] = paramTypes[i].getName();
				}
				opkeyToMethod.put(new OperationKey(opName, paramTypesNames), method);

			}
		}
		return opkeyToMethod;
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

	private static final class DynamicMBeanAggregator implements DynamicMBean {
		private final MBeanInfo mBeanInfo;
		private final List<JmxMonitorableWrapper> wrappers;
		private final Map<String, Class<? extends JmxStats<?>>> nameToJmxStatsType;

		public DynamicMBeanAggregator(MBeanInfo mBeanInfo, List<JmxMonitorableWrapper> wrappers,
		                              Map<String, Class<? extends JmxStats<?>>> nameToJmxStatsType) {
			this.mBeanInfo = mBeanInfo;
			this.wrappers = wrappers;
			this.nameToJmxStatsType = nameToJmxStatsType;
		}

		@SuppressWarnings("unchecked")
		@Override
		public Object getAttribute(String attribute) throws AttributeNotFoundException, MBeanException, ReflectionException {
			Matcher matcher = ATTRIBUTE_NAME_PATTERN.matcher(attribute);
			if (matcher.matches()) {
				String jmxStatsName = matcher.group(1);
				Class<? extends JmxStats<?>> jmxStatsType = nameToJmxStatsType.get(jmxStatsName);
				// wildcard is removed intentionally, types must be same
				JmxStats statsAccumulator;
				try {
					statsAccumulator = jmxStatsType.newInstance();
				} catch (InstantiationException | IllegalAccessException e) {
					String errorMsg = "All JmxStats Implementations must have public constructor without parameters";
					throw new ReflectionException(e, errorMsg);
				}
				for (JmxMonitorableWrapper wrapper : wrappers) {
					statsAccumulator.add(wrapper.getJmxStats(jmxStatsName));
				}
				String attrNameRest = matcher.group(2);
				Map<String, JmxStats.TypeAndValue> jmxStatsAttributes = statsAccumulator.getAttributes();
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
			for (JmxMonitorableWrapper wrapper : wrappers) {
				wrapper.invoke(actionName, params, signature);
			}
			// We don't know how to aggregate return values from several monitorables
			return null;
		}

		@Override
		public MBeanInfo getMBeanInfo() {
			return mBeanInfo;
		}
	}

	private static final class JmxMonitorableWrapper {
		private final Object monitorable;
		private final Map<String, Method> attributeGetters;
		private final Map<OperationKey, Method> operationKeyToMethod;

		public JmxMonitorableWrapper(Object monitorable, Map<String, Method> attributeGetters,
		                             Map<OperationKey, Method> operationKeyToMethod) {
			this.monitorable = monitorable;
			this.attributeGetters = attributeGetters;
			this.operationKeyToMethod = operationKeyToMethod;
		}

		public JmxStats<?> getJmxStats(String jmxStatsName) throws AttributeNotFoundException, ReflectionException {
			Method jmxStatsGetter = attributeGetters.get(jmxStatsName);
			if (jmxStatsGetter == null) {
				throw new AttributeNotFoundException();
			}
			try {
				return (JmxStats<?>) jmxStatsGetter.invoke(monitorable);
			} catch (IllegalAccessException | InvocationTargetException e) {
				throw new ReflectionException(e);
			}
		}

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
	}
}
