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

import io.datakernel.eventloop.Eventloop;

import javax.management.*;
import java.lang.annotation.Annotation;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static io.datakernel.jmx.Utils.*;
import static io.datakernel.util.Preconditions.checkArgument;
import static io.datakernel.util.Preconditions.checkNotNull;
import static java.lang.String.format;

public final class JmxMBeans implements DynamicMBeanFactory {
	private static final String ATTRIBUTE_NAME_FORMAT = "%s_%s";
	private static final String ATTRIBUTE_NAME_REGEX = "^([a-zA-Z0-9]+)_(\\w+)$";
	private static final Pattern ATTRIBUTE_NAME_PATTERN = Pattern.compile(ATTRIBUTE_NAME_REGEX);
	private static final String ATTRIBUTE_DEFAULT_DESCRIPTION = "";

	private static final String REFRESH_PERIOD_ATTRIBUTE_NAME = "_refreshPeriod";
	private static final String SMOOTHING_WINDOW_ATTRIBUTE_NAME = "_smoothingWindow";
	private static final String SET_REFRESH_PERIOD_OP_NAME = "_setRefreshPeriod";
	private static final String SET_SMOOTHING_WINDOW_OP_NAME = "_setSmoothingWindow";
	private static final String SET_REFRESH_PERIOD_PARAMETER_NAME = "period";
	private static final String SET_SMOOTHING_WINDOW_PARAMETER_NAME = "window";

	public static final double DEFAULT_REFRESH_PERIOD = 0.2;
	public static final double DEFAULT_SMOOTHING_WINDOW = 10.0;

	private static final JmxMBeans factory = new JmxMBeans();

	private JmxMBeans() {

	}

	public static JmxMBeans factory() {
		return factory;
	}

	@Override
	public DynamicMBean createFor(List<?> monitorables) throws Exception {
		return createFor(monitorables, false);
	}

	@Override
	public DynamicMBean createFor(List<?> monitorables, boolean enableRefresh) throws Exception {
		checkNotNull(monitorables);
		checkArgument(monitorables.size() > 0);
		checkArgument(!listContainsNullValues(monitorables), "monitorable can not be null");
		checkArgument(allObjectsAreOfSameType(monitorables));
		checkArgument(allObjectsAreAnnotatedWithJmxMBean(monitorables),
				"objects for DynamicMBean creation should be of type annotated with @JmxMBean");

		// all objects are of same type, so we can extract info from any of them
		Object first = monitorables.get(0);
		MBeanInfo mBeanInfo = composeMBeanInfo(first, enableRefresh);
		Map<String, Class<? extends JmxStats<?>>> nameToJmxStatsType = fetchNameToJmxStatsType(first);

		boolean eventloopMonitorables = hasEventloop(first);
		List<Eventloop> eventloops = new ArrayList<>();
		if (eventloopMonitorables) {
			for (Object monitorable : monitorables) {
				eventloops.add(fetchEventloop(monitorable));
			}
		}

		List<JmxMonitorableWrapper> wrappers = new ArrayList<>();
		for (int i = 0; i < monitorables.size(); i++) {
			Object monitorable = monitorables.get(i);
			StatsRefresher statsRefresher = null;
			if (enableRefresh && eventloopMonitorables) {
				Eventloop eventloop = eventloops.get(i);
				List<JmxStats<?>> statsList = new ArrayList<>(fetchNameToJmxStats(monitorable).values());
				statsRefresher =
						new StatsRefresher(statsList, DEFAULT_REFRESH_PERIOD, DEFAULT_SMOOTHING_WINDOW, eventloop);
				eventloop.postConcurrently(statsRefresher);
			}
			wrappers.add(createWrapper(monitorable, statsRefresher));
		}

		return new DynamicMBeanAggregator(mBeanInfo, wrappers, nameToJmxStatsType,
				DEFAULT_REFRESH_PERIOD, DEFAULT_SMOOTHING_WINDOW);
	}

	private static MBeanAttributeInfo[] extractAttributesInfo(Object monitorable, boolean enableRefresh)
			throws InvocationTargetException, IllegalAccessException {
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

		if (enableRefresh) {
			addAttributesForRefreshControl(monitorable, attributes);
		}

		return attributes.toArray(new MBeanAttributeInfo[attributes.size()]);
	}

	private static void addAttributesForRefreshControl(Object monitorable, List<MBeanAttributeInfo> attributes)
			throws InvocationTargetException, IllegalAccessException {
		if (hasEventloop(monitorable)) {
			MBeanAttributeInfo refreshPeriodAttr =
					new MBeanAttributeInfo(REFRESH_PERIOD_ATTRIBUTE_NAME, "double", ATTRIBUTE_DEFAULT_DESCRIPTION,
							true, false, false);
			attributes.add(refreshPeriodAttr);
			MBeanAttributeInfo smoothingWindowAttr =
					new MBeanAttributeInfo(SMOOTHING_WINDOW_ATTRIBUTE_NAME, "double", ATTRIBUTE_DEFAULT_DESCRIPTION,
							true, false, false);
			attributes.add(smoothingWindowAttr);
		}
	}

	private static MBeanOperationInfo[] extractOperationsInfo(Object monitorable, boolean enableRefresh)
			throws InvocationTargetException, IllegalAccessException {
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

		if (enableRefresh) {
			addOperationsForRefreshControl(monitorable, operations);
		}

		return operations.toArray(new MBeanOperationInfo[operations.size()]);
	}

	private static void addOperationsForRefreshControl(Object monitorable, List<MBeanOperationInfo> operations)
			throws InvocationTargetException, IllegalAccessException {
		if (hasEventloop(monitorable)) {
			MBeanParameterInfo[] setPeriodOpParameters = new MBeanParameterInfo[]{
					new MBeanParameterInfo(SET_REFRESH_PERIOD_PARAMETER_NAME, "double", "")};
			MBeanOperationInfo setPeriodOp = new MBeanOperationInfo(
					SET_REFRESH_PERIOD_OP_NAME, "", setPeriodOpParameters, "void", MBeanOperationInfo.ACTION);
			operations.add(setPeriodOp);

			MBeanParameterInfo[] setWindowOpParameters = new MBeanParameterInfo[]{
					new MBeanParameterInfo(SET_SMOOTHING_WINDOW_PARAMETER_NAME, "double", "")};
			MBeanOperationInfo setSmoothingWindowOp = new MBeanOperationInfo(
					SET_SMOOTHING_WINDOW_OP_NAME, "", setWindowOpParameters, "void", MBeanOperationInfo.ACTION);
			operations.add(setSmoothingWindowOp);
		}
	}

	private static boolean hasEventloop(Object monitorable) throws InvocationTargetException, IllegalAccessException {
		return tryFetchEventloop(monitorable) != null;
	}

	private static Eventloop fetchEventloop(Object monitorable)
			throws InvocationTargetException, IllegalAccessException {
		Eventloop eventloop = tryFetchEventloop(monitorable);
		if (eventloop != null) {
			return eventloop;
		}
		throw new IllegalArgumentException("monitorable doesn't contain eventloop");
	}

	/**
	 * Returns Eventloop getter if monitorable has it, otherwise returns null
	 */
	private static Eventloop tryFetchEventloop(Object monitorable)
			throws InvocationTargetException, IllegalAccessException {
		if (Eventloop.class.isAssignableFrom(monitorable.getClass())) {
			return (Eventloop) monitorable;
		}

		for (Method method : monitorable.getClass().getMethods()) {
			boolean nameMatches = method.getName().toLowerCase().equals("geteventloop");
			boolean returnTypeMatches = Eventloop.class.isAssignableFrom(method.getReturnType());
			boolean hasNoArgs = method.getParameterTypes().length == 0;
			if (nameMatches && returnTypeMatches && hasNoArgs) {
				return (Eventloop) method.invoke(monitorable);
			}
		}
		return null;
	}

	private static JmxParameter findJmxNamedParameterAnnotation(Annotation[] annotations) {
		for (Annotation annotation : annotations) {
			if (annotation.annotationType().equals(JmxParameter.class)) {
				return (JmxParameter) annotation;
			}
		}
		return null;
	}

	private static <T> boolean listContainsNullValues(List<T> list) {
		for (int i = 0; i < list.size(); i++) {
			if (list.get(i) == null) {
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

	private static MBeanInfo composeMBeanInfo(Object monitorable, boolean enableRefresh)
			throws InvocationTargetException, IllegalAccessException {
		String monitorableName = "";
		String monitorableDescription = "";
		MBeanAttributeInfo[] attributes = extractAttributesInfo(monitorable, enableRefresh);
		MBeanOperationInfo[] operations = extractOperationsInfo(monitorable, enableRefresh);
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

	private static JmxMonitorableWrapper createWrapper(Object monitorable, StatsRefresher statsRefresher) {
		Map<String, Method> attributeGetters = fetchNameToJmxStatsGetter(monitorable);
		Map<OperationKey, Method> opkeyToMethod = fetchOpkeyToMethod(monitorable);
		return new JmxMonitorableWrapper(monitorable, attributeGetters, opkeyToMethod, statsRefresher);
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
		private double refreshPeriod;
		private double smoothingWindow;

		public DynamicMBeanAggregator(MBeanInfo mBeanInfo, List<JmxMonitorableWrapper> wrappers,
		                              Map<String, Class<? extends JmxStats<?>>> nameToJmxStatsType,
		                              double refreshPeriod, double smoothingWindow) {
			this.mBeanInfo = mBeanInfo;
			this.wrappers = wrappers;
			this.nameToJmxStatsType = nameToJmxStatsType;
			this.refreshPeriod = refreshPeriod;
			this.smoothingWindow = smoothingWindow;
		}

		@SuppressWarnings("unchecked")
		@Override
		public Object getAttribute(String attribute) throws AttributeNotFoundException, MBeanException, ReflectionException {
			if (attribute.equals(REFRESH_PERIOD_ATTRIBUTE_NAME)) {
				return refreshPeriod;
			} else if (attribute.equals(SMOOTHING_WINDOW_ATTRIBUTE_NAME)) {
				return smoothingWindow;
			}

			Matcher matcher = ATTRIBUTE_NAME_PATTERN.matcher(attribute);
			if (matcher.matches()) {
				String jmxStatsName = matcher.group(1);
				if (!nameToJmxStatsType.containsKey(jmxStatsName)) {
					throw new AttributeNotFoundException();
				}
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
		public Object invoke(String actionName, Object[] params, String[] signature)
				throws MBeanException, ReflectionException {
			if (execIfActionIsForRefreshControl(actionName, params, signature)) {
				return null;
			}

			for (JmxMonitorableWrapper wrapper : wrappers) {
				wrapper.invoke(actionName, params, signature);
			}
			// We don't know how to aggregate return values from several monitorables
			return null;
		}

		/**
		 * Returns true and perform action if action is for refresh control, otherwise returns false
		 */
		private boolean execIfActionIsForRefreshControl(String actionName, Object[] params, String[] signature)
				throws MBeanException {
			boolean singleArgDouble =
					hasSingleElement(signature) && signature[0].equals("double") && params[0] != null;
			if (actionName.equals(SET_REFRESH_PERIOD_OP_NAME) && singleArgDouble) {
				refreshPeriod = (double) params[0];
				for (JmxMonitorableWrapper wrapper : wrappers) {
					try {
						wrapper.setRefreshPeriod(refreshPeriod);
					} catch (IllegalStateException e) {
						throw new MBeanException(e, "Cannot set refresh period");
					}
				}
				return true;
			} else if (actionName.equals(SET_SMOOTHING_WINDOW_OP_NAME) && singleArgDouble) {
				smoothingWindow = (double) params[0];
				for (JmxMonitorableWrapper wrapper : wrappers) {
					try {
						wrapper.setSmoothingWindow(smoothingWindow);
					} catch (IllegalStateException e) {
						throw new MBeanException(e, "Cannot set refresh period");
					}
				}
				return true;
			}

			return false;
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
		private final StatsRefresher statsRefresher;

		public JmxMonitorableWrapper(Object monitorable, Map<String, Method> attributeGetters,
		                             Map<OperationKey, Method> operationKeyToMethod, StatsRefresher statsRefresher) {
			this.monitorable = monitorable;
			this.attributeGetters = attributeGetters;
			this.operationKeyToMethod = operationKeyToMethod;
			this.statsRefresher = statsRefresher;
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
				String operationName = prettyOperationName(actionName, argTypes);
				String errorMsg = "There is no operation \"" + operationName + "\"";
				throw new RuntimeOperationsException(new IllegalArgumentException("Operation not found"), errorMsg);
			}
			try {
				Object[] args = params != null ? params : new Object[0];
				return opMethod.invoke(monitorable, args);
			} catch (IllegalAccessException | InvocationTargetException e) {
				throw new ReflectionException(e);
			}
		}

		private static String prettyOperationName(String name, String[] argTypes) {
			String operationName = name + "(";
			if (argTypes.length > 0) {
				for (int i = 0; i < argTypes.length - 1; i++) {
					operationName += argTypes[i] + ", ";
				}
				operationName += argTypes[argTypes.length - 1];
			}
			operationName += ")";
			return operationName;
		}

		public void setRefreshPeriod(double refreshPeriod) {
			if (statsRefresher == null) {
				throw new IllegalStateException("Cannot set refresh period, statsManager is null");
			}
			statsRefresher.setPeriod(refreshPeriod);
		}

		public void setSmoothingWindow(double smoothingWindow) {
			if (statsRefresher == null) {
				throw new IllegalStateException("Cannot set smoothing period, statsManager is null");
			}
			statsRefresher.setSmoothingWindow(smoothingWindow);
		}
	}
}
