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
import io.datakernel.time.CurrentTimeProvider;
import io.datakernel.time.CurrentTimeProviderSystem;

import javax.management.*;
import javax.management.openmbean.*;
import java.lang.annotation.Annotation;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.*;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static io.datakernel.jmx.RefreshTaskPerPool.ExecutorAndStatsList;
import static io.datakernel.jmx.Utils.*;
import static io.datakernel.util.Preconditions.*;
import static java.lang.String.format;

public final class JmxMBeans implements DynamicMBeanFactory {
	private static final String ATTRIBUTE_NAME_FORMAT = "%s_%s";
	private static final String ATTRIBUTE_NAME_REGEX = "^([a-zA-Z0-9]+)_(\\w+)$";
	private static final Pattern ATTRIBUTE_NAME_PATTERN = Pattern.compile(ATTRIBUTE_NAME_REGEX);
	private static final String ATTRIBUTE_DEFAULT_DESCRIPTION = "";

	private static final Timer TIMER = new Timer(true);
	private static final String REFRESH_PERIOD_ATTRIBUTE_NAME = "_refreshPeriod";
	private static final String SMOOTHING_WINDOW_ATTRIBUTE_NAME = "_smoothingWindow";
	private static final String SET_REFRESH_PERIOD_OP_NAME = "_setRefreshPeriod";
	private static final String SET_SMOOTHING_WINDOW_OP_NAME = "_setSmoothingWindow";
	private static final String SET_REFRESH_PERIOD_PARAMETER_NAME = "period";
	private static final String SET_SMOOTHING_WINDOW_PARAMETER_NAME = "window";

	// CompositeData of Throwable
	private static final String THROWABLE_COMPOSITE_TYPE_NAME = "CompositeDataOfThrowable";
	private static final String THROWABLE_TYPE_KEY = "type";
	private static final String THROWABLE_MESSAGE_KEY = "message";
	private static final String THROWABLE_CAUSE_KEY = "cause";
	private static final String THROWABLE_STACK_TRACE_KEY = "stackTrace";

	public static final double DEFAULT_REFRESH_PERIOD = 0.2;
	public static final double DEFAULT_SMOOTHING_WINDOW = 10.0;

	private static final JmxMBeans factory = new JmxMBeans();

	private static final CurrentTimeProvider TIME_PROVIDER = CurrentTimeProviderSystem.instance();

	private static CompositeType compositeTypeOfThrowable;

	private JmxMBeans() {}

	public static JmxMBeans factory() {
		return factory;
	}

	@Override
	public DynamicMBean createFor(List<? extends ConcurrentJmxMBean> monitorables, boolean enableRefresh)
			throws Exception {
		checkNotNull(monitorables);
		checkArgument(monitorables.size() > 0);
		checkArgument(!listContainsNullValues(monitorables), "monitorable can not be null");
		checkArgument(allObjectsAreOfSameType(monitorables));

		// all objects are of same type, so we can extract info from any of them
		Object first = monitorables.get(0);
		MBeanInfo mBeanInfo = composeMBeanInfo(first, enableRefresh);
		Map<String, Class<? extends JmxStats<?>>> nameToJmxStatsType = fetchNameToJmxStatsType(first);
		Map<String, MBeanAttributeInfo> nameToSimpleAttribute = fetchNameToSimpleAttribute(first);
		Set<String> listAttributeNames = fetchNameToListAttributeGetter(first).keySet();
		Set<String> arrayAttributeNames = fetchNameToArrayAttributeGetter(first).keySet();
		Set<String> throwableAttributeNames = fetchNameToThrowableAttributeGetter(first).keySet();

		List<SimpleAttribute> writableAttributes = collectWritableAttributes(first);

		List<JmxMonitorableWrapper> wrappers = new ArrayList<>();
		for (int i = 0; i < monitorables.size(); i++) {
			wrappers.add(createWrapper(monitorables.get(i), writableAttributes));
		}

		DynamicMBeanAggregator mbean = new DynamicMBeanAggregator(mBeanInfo, wrappers, nameToJmxStatsType, nameToSimpleAttribute,
				listAttributeNames, arrayAttributeNames, throwableAttributeNames, enableRefresh);

		if (enableRefresh) {
			mbean.startRefreshing(DEFAULT_REFRESH_PERIOD, DEFAULT_SMOOTHING_WINDOW);
		}

		return mbean;
	}

	private static List<ExecutorAndStatsList> fetchExecutorsAndStatsLists(List<? extends ConcurrentJmxMBean> monitorables) {
		List<ExecutorAndStatsList> executorsAndStatsLists = new ArrayList<>(monitorables.size());
		for (ConcurrentJmxMBean monitorable : monitorables) {
			executorsAndStatsLists.add(new ExecutorAndStatsList(monitorable.getJmxExecutor(),
					new ArrayList<>(fetchNameToJmxStats(monitorable).values())));
		}
		return executorsAndStatsLists;
	}

	private static List<SimpleAttribute> collectWritableAttributes(Object monitorable) {
		Method[] methods = monitorable.getClass().getMethods();
		List<SimpleAttribute> writableAttributes = new ArrayList<>();
		for (Method method : methods) {
			if (method.isAnnotationPresent(JmxAttribute.class) && isGetterOfSimpleType(method)) {
				String name = extractFieldNameFromGetter(method);
				boolean writable = doesSetterForAttributeExist(monitorable, name, method.getReturnType());
				if (writable) {
					writableAttributes.add(new SimpleAttribute(name, method.getReturnType()));
				}
			}
		}
		return writableAttributes;
	}

	private static Map<String, MBeanAttributeInfo> fetchNameToSimpleAttribute(Object monitorable) {
		List<MBeanAttributeInfo> simpleAttrs = fetchSimpleAttributesInfo(monitorable);
		Map<String, MBeanAttributeInfo> nameToSimpleAttribute = new HashMap<>();
		for (MBeanAttributeInfo simpleAttr : simpleAttrs) {
			nameToSimpleAttribute.put(simpleAttr.getName(), simpleAttr);
		}
		return nameToSimpleAttribute;
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

		List<MBeanAttributeInfo> simpleAttrs = fetchSimpleAttributesInfo(monitorable);
		attributes.addAll(simpleAttrs);

		List<MBeanAttributeInfo> listAttrs = fetchListAttributesInfo(monitorable);
		attributes.addAll(listAttrs);

		List<MBeanAttributeInfo> arrayAttrs = fetchArrayAttributesInfo(monitorable);
		attributes.addAll(arrayAttrs);

		List<MBeanAttributeInfo> exceptionAttrs = fetchExceptionAttributesInfo(monitorable);
		attributes.addAll(exceptionAttrs);

		if (enableRefresh) {
			addAttributesForRefreshControl(attributes);
		}

		return attributes.toArray(new MBeanAttributeInfo[attributes.size()]);
	}

	/**
	 * Simple attributes means that attribute's type is of one of the following: boolean, int, long, double, String
	 */
	private static List<MBeanAttributeInfo> fetchSimpleAttributesInfo(Object monitorable) {
		List<MBeanAttributeInfo> attrList = new ArrayList<>();
		Method[] methods = monitorable.getClass().getMethods();
		for (Method method : methods) {
			if (method.isAnnotationPresent(JmxAttribute.class) && isGetterOfSimpleType(method)) {
				String name = extractFieldNameFromGetter(method);
				boolean writable = doesSetterForAttributeExist(monitorable, name, method.getReturnType());
				String type = method.getReturnType().getName();
				MBeanAttributeInfo attrInfo =
						new MBeanAttributeInfo(name, type, ATTRIBUTE_DEFAULT_DESCRIPTION,
								true, writable, false);
				attrList.add(attrInfo);
			}
		}
		return attrList;
	}

	/**
	 * fetch attributes of type java.util.List
	 */
	private static List<MBeanAttributeInfo> fetchListAttributesInfo(Object monitorable) {
		List<MBeanAttributeInfo> attrList = new ArrayList<>();
		Method[] methods = monitorable.getClass().getMethods();
		for (Method method : methods) {
			if (method.isAnnotationPresent(JmxAttribute.class) && isGetterOfList(method)) {
				String name = extractFieldNameFromGetter(method);
				String type = String[].class.getName();
				MBeanAttributeInfo attrInfo =
						new MBeanAttributeInfo(name, type, ATTRIBUTE_DEFAULT_DESCRIPTION,
								true, false, false);
				attrList.add(attrInfo);
			}
		}
		return attrList;
	}

	private static List<MBeanAttributeInfo> fetchArrayAttributesInfo(Object monitorable) {
		List<MBeanAttributeInfo> attrList = new ArrayList<>();
		Method[] methods = monitorable.getClass().getMethods();
		for (Method method : methods) {
			if (method.isAnnotationPresent(JmxAttribute.class) && isGetterOfArray(method)) {
				String name = extractFieldNameFromGetter(method);
				String type = String[].class.getName();
				MBeanAttributeInfo attrInfo =
						new MBeanAttributeInfo(name, type, ATTRIBUTE_DEFAULT_DESCRIPTION,
								true, false, false);
				attrList.add(attrInfo);
			}
		}
		return attrList;
	}

	private static List<MBeanAttributeInfo> fetchExceptionAttributesInfo(Object monitorable) {
		List<MBeanAttributeInfo> attrList = new ArrayList<>();
		Method[] methods = monitorable.getClass().getMethods();
		for (Method method : methods) {
			if (method.isAnnotationPresent(JmxAttribute.class) && isGetterOfThrowable(method)) {
				String name = extractFieldNameFromGetter(method);
				String type = THROWABLE_COMPOSITE_TYPE_NAME;
				MBeanAttributeInfo attrInfo =
						new MBeanAttributeInfo(name, type, ATTRIBUTE_DEFAULT_DESCRIPTION,
								true, false, false);
				attrList.add(attrInfo);
			}
		}
		return attrList;
	}

	private static CompositeType getCompositeTypeOfThrowable() throws OpenDataException {
		if (compositeTypeOfThrowable == null) {
			String[] itemNames = new String[]{
					THROWABLE_TYPE_KEY,
					THROWABLE_MESSAGE_KEY,
					THROWABLE_CAUSE_KEY,
					THROWABLE_STACK_TRACE_KEY
			};
			OpenType<?>[] itemTypes = new OpenType<?>[]{
					SimpleType.STRING,
					SimpleType.STRING,
					SimpleType.STRING,
					new ArrayType<>(1, SimpleType.STRING)
			};

			compositeTypeOfThrowable = new CompositeType(
					THROWABLE_COMPOSITE_TYPE_NAME,
					THROWABLE_COMPOSITE_TYPE_NAME,
					itemNames,
					itemNames,
					itemTypes);
		}
		return compositeTypeOfThrowable;
	}

	/**
	 * Tries to fetch setters for writable attributes. Throws exception if there is no setter for writable attribute
	 * Ignores read-only attributes
	 */
	private static Map<String, Method> fetchNameToSimpleAttributeSetter(
			Object monitorable, List<SimpleAttribute> writableAttributes) {
		Map<String, Method> simpleAttributeSetters = new HashMap<>();
		for (SimpleAttribute attribute : writableAttributes) {
			String attrName = attribute.getName();
			Class<?> attrType = attribute.getType();
			Method setter = tryFetchSetter(monitorable, attrName, attrType);
			if (setter == null) {
				throw new IllegalArgumentException(
						format("Cannot fetch setter for writable attribute with name \"%s\" and type \"%s\"",
								attrName, attrType.toString())
				);
			}
			simpleAttributeSetters.put(attrName, setter);
		}
		return simpleAttributeSetters;
	}

	private static boolean doesSetterForAttributeExist(Object monitorable, String name, Class<?> type) {
		Method[] methods = monitorable.getClass().getMethods();
		for (Method method : methods) {
			if (method.isAnnotationPresent(JmxAttribute.class) && isSetterOf(method, name, type)) {
				return true;
			}
		}
		return false;
	}

	private static Method tryFetchSetter(Object monitorable, String name, Class<?> type) {
		Method[] methods = monitorable.getClass().getMethods();
		for (Method method : methods) {
			if (method.isAnnotationPresent(JmxAttribute.class) && isSetterOf(method, name, type)) {
				return method;
			}
		}
		return null;
	}

	private static boolean isSetterOf(Method method, String name, Class<?> type) {
		if (isSetter(method)) {
			String methodName = method.getName();
			String attrName = methodName.substring(3, 4).toLowerCase() + methodName.substring(4);
			return attrName.equals(name) && method.getParameterTypes()[0].equals(type);
		}
		return false;
	}

	public static boolean isSetter(Method method) {
		String methodName = method.getName();
		return methodName.length() > 3 && methodName.startsWith("set")
				&& method.getReturnType().equals(void.class) && method.getParameterTypes().length == 1;
	}

	private static void addAttributesForRefreshControl(List<MBeanAttributeInfo> attributes)
			throws InvocationTargetException, IllegalAccessException {
		MBeanAttributeInfo refreshPeriodAttr =
				new MBeanAttributeInfo(REFRESH_PERIOD_ATTRIBUTE_NAME, "double", ATTRIBUTE_DEFAULT_DESCRIPTION,
						true, false, false);
		attributes.add(refreshPeriodAttr);
		MBeanAttributeInfo smoothingWindowAttr =
				new MBeanAttributeInfo(SMOOTHING_WINDOW_ATTRIBUTE_NAME, "double", ATTRIBUTE_DEFAULT_DESCRIPTION,
						true, false, false);
		attributes.add(smoothingWindowAttr);
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
			addOperationsForRefreshControl(operations);
		}

		return operations.toArray(new MBeanOperationInfo[operations.size()]);
	}

	private static void addOperationsForRefreshControl(List<MBeanOperationInfo> operations)
			throws InvocationTargetException, IllegalAccessException {
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

	private static JmxMonitorableWrapper createWrapper(
			ConcurrentJmxMBean monitorable, List<SimpleAttribute> writableAttributes) {
		Map<String, Method> nameToJmxStatsGetter = fetchNameToJmxStatsGetter(monitorable);
		List<? extends JmxStats<?>> jmxStatsList = new ArrayList<>(fetchNameToJmxStats(monitorable).values());
		Map<String, Method> nameToSimpleAttributeGetter = fetchNameToSimpleAttributeGetter(monitorable);
		Map<String, Method> nameToSimpleAttributeSetter =
				fetchNameToSimpleAttributeSetter(monitorable, writableAttributes);
		Map<String, Method> nameToListAttributeGetter = fetchNameToListAttributeGetter(monitorable);
		Map<String, Method> nameToArrayAttributeGetter = fetchNameToArrayAttributeGetter(monitorable);
		Map<String, Method> nameToThrowableAttributeGetter = fetchNameToThrowableAttributeGetter(monitorable);
		Map<OperationKey, Method> opkeyToMethod = fetchOpkeyToMethod(monitorable);
		return new JmxMonitorableWrapper(monitorable,
				nameToJmxStatsGetter,
				jmxStatsList,
				nameToSimpleAttributeGetter, nameToSimpleAttributeSetter,
				nameToListAttributeGetter,
				nameToArrayAttributeGetter,
				nameToThrowableAttributeGetter,
				opkeyToMethod);
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

	private static final class SimpleAttribute {
		private final String name;
		private final Class<?> type;

		public SimpleAttribute(String name, Class<?> type) {
			this.name = name;
			this.type = type;
		}

		public String getName() {
			return name;
		}

		public Class<?> getType() {
			return type;
		}
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
		private final Map<String, MBeanAttributeInfo> nameToSimpleAttribute;
		private final Set<String> listAttributes;
		private final Set<String> arrayAttributes;
		private final Set<String> exceptionAttributes;

		// refresh
		private volatile double refreshPeriod;
		private volatile double smoothingWindow;
		private boolean refreshEnabled;

		public DynamicMBeanAggregator(MBeanInfo mBeanInfo, List<JmxMonitorableWrapper> wrappers,
		                              Map<String, Class<? extends JmxStats<?>>> nameToJmxStatsType,
		                              Map<String, MBeanAttributeInfo> nameToSimpleAttribute,
		                              Set<String> listAttributes, Set<String> arrayAttributes,
		                              Set<String> exceptionAttributes, boolean enableRefresh) {
			this.mBeanInfo = mBeanInfo;
			this.wrappers = wrappers;
			this.nameToJmxStatsType = nameToJmxStatsType;
			this.nameToSimpleAttribute = nameToSimpleAttribute;
			this.listAttributes = listAttributes;
			this.arrayAttributes = arrayAttributes;
			this.exceptionAttributes = exceptionAttributes;
			this.refreshEnabled = enableRefresh;
		}

		public void startRefreshing(double defaultRefreshPeriod, double defaultSmoothingWindow) {
			checkState(refreshEnabled());
			refreshPeriod = defaultRefreshPeriod;
			smoothingWindow = defaultSmoothingWindow;
			TIMER.schedule(createRefreshTask(), 0L);
		}

		private TimerTask createRefreshTask() {
			checkState(refreshEnabled());
			return new TimerTask() {
				@Override
				public void run() {
					final AtomicInteger mbeansLeftForRefresh = new AtomicInteger(wrappers.size());
					// cache smoothingWindow and refreshPeriod to be same for all localRefreshTasks
					// because this two parameters may be changed from other thread
					final double currentSmoothingWindow = smoothingWindow;
					final int currentRefreshPeriod = secondsToMillis(refreshPeriod);
					final long currentTimestamp = TIME_PROVIDER.currentTimeMillis();
					for (JmxMonitorableWrapper wrapper : wrappers) {
						final Executor executor = wrapper.getExecutor();
						checkNotNull(executor, "Error. Executor of ConcurrentMBean cannot be null");
						final List<? extends JmxStats<?>> statsList = wrapper.getAllJmxStats();
						executor.execute(new Runnable() {
							@Override
							public void run() {
								for (JmxStats<?> jmxStats : statsList) {
									jmxStats.refreshStats(currentTimestamp, currentSmoothingWindow);
								}
								int tasksLeft = mbeansLeftForRefresh.decrementAndGet();
								if (tasksLeft == 0) {
									TIMER.schedule(createRefreshTask(), currentRefreshPeriod);
								}
							}
						});
					}
				}
			};
		}

		private static int secondsToMillis(double seconds) {
			return (int) (seconds * 1000);
		}

		@SuppressWarnings("unchecked")
		@Override
		public Object getAttribute(String attribute) throws AttributeNotFoundException, MBeanException, ReflectionException {
			if (attribute.equals(REFRESH_PERIOD_ATTRIBUTE_NAME)) {
				checkState(refreshEnabled());
				return refreshPeriod;
			} else if (attribute.equals(SMOOTHING_WINDOW_ATTRIBUTE_NAME)) {
				checkState(refreshEnabled());
				return smoothingWindow;
			}

			if (isJmxStatsAttribute(attribute)) {
				return aggregateJmxStatsAttribute(attribute);
			} else if (nameToSimpleAttribute.containsKey(attribute)) {
				return aggregateSimpleTypeAttribute(attribute);
			} else if (listAttributes.contains(attribute)) {
				return aggregateListAttribute(attribute);
			} else if (arrayAttributes.contains(attribute)) {
				return aggregateArrayAttribute(attribute);
			} else if (exceptionAttributes.contains(attribute)) {
				try {
					return buildCompositeDataForThrowable(aggregateExceptionAttribute(attribute));
				} catch (OpenDataException e) {
					throw new MBeanException(
							e, format("Cannot create CompositeData for Throwable with name \"%s\"", attribute));
				}
			} else {
				throw new AttributeNotFoundException();
			}
		}

		private boolean isJmxStatsAttribute(String attrName) {
			Matcher matcher = ATTRIBUTE_NAME_PATTERN.matcher(attrName);
			if (matcher.matches()) {
				String jmxStatsName = matcher.group(1);
				return nameToJmxStatsType.containsKey(jmxStatsName);
			} else {
				return false;
			}
		}

		private Object aggregateJmxStatsAttribute(String attribute)
				throws ReflectionException, AttributeNotFoundException, MBeanException {
			Matcher matcher = ATTRIBUTE_NAME_PATTERN.matcher(attribute);
			if (matcher.matches()) {
				// attribute is JmxStats
				String attrName = matcher.group(1);
				if (nameToJmxStatsType.containsKey(attrName)) {
					Class<? extends JmxStats<?>> jmxStatsType = nameToJmxStatsType.get(attrName);
					// wildcard is removed intentionally, types must be same
					JmxStats statsAccumulator;
					try {
						statsAccumulator = jmxStatsType.newInstance();
					} catch (InstantiationException | IllegalAccessException e) {
						String errorMsg = "All JmxStats Implementations must have public constructor without parameters";
						throw new ReflectionException(e, errorMsg);
					}
					for (JmxMonitorableWrapper wrapper : wrappers) {
						statsAccumulator.add(wrapper.getJmxStats(attrName));
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
			} else {
				throw new MBeanException(new IllegalStateException(
						format("Error. JmxStats attribute with name \"%s\" " +
								"does not conform naming convention", attribute)
				));
			}
		}

		private Object aggregateSimpleTypeAttribute(String attrName)
				throws AttributeNotFoundException, ReflectionException, MBeanException {
			JmxMonitorableWrapper first = wrappers.get(0);
			Object attrValue = first.getSimpleAttributeValue(attrName);
			for (int i = 1; i < wrappers.size(); i++) {
				Object currentAttrValue = wrappers.get(i).getSimpleAttributeValue(attrName);
				if (!Objects.equals(attrValue, currentAttrValue)) {
					throw new MBeanException(new Exception("Attribute values in pool instances are different"));
				}
			}
			return attrValue;
		}

		private String[] aggregateListAttribute(String attrName)
				throws AttributeNotFoundException, ReflectionException {
			List<String> allItems = new ArrayList<>();
			for (JmxMonitorableWrapper wrapper : wrappers) {
				List<?> currentList = wrapper.getListAttributeValue(attrName);
				if (currentList != null) {
					for (Object o : currentList) {
						String item = o != null ? o.toString() : "null";
						allItems.add(item);
					}
				}
			}
			return allItems.toArray(new String[allItems.size()]);
		}

		private boolean refreshEnabled() {
			return refreshEnabled;
		}

		// TODO(vmykhalko): refactor this method - it resembles aggregateListAttribute()
		private String[] aggregateArrayAttribute(String attrName)
				throws AttributeNotFoundException, ReflectionException {
			List<String> allItems = new ArrayList<>();
			for (JmxMonitorableWrapper wrapper : wrappers) {
				Object[] currentArray = wrapper.getArrayAttributeValue(attrName);
				if (currentArray != null) {
					for (Object o : currentArray) {
						String item = o != null ? o.toString() : "null";
						allItems.add(item);
					}
				}
			}
			return allItems.toArray(new String[allItems.size()]);
		}

		private Throwable aggregateExceptionAttribute(String attrName)
				throws AttributeNotFoundException, ReflectionException, MBeanException {
			JmxMonitorableWrapper first = wrappers.get(0);
			Throwable throwable = first.getThrowableAttributeValue(attrName);
			for (int i = 1; i < wrappers.size(); i++) {
				Throwable currentThrowable = wrappers.get(i).getThrowableAttributeValue(attrName);
				if (!Objects.equals(throwable, currentThrowable)) {
					throw new MBeanException(new Exception("Throwables in pool instances are different"));
				}
			}
			return throwable;
		}

		private CompositeData buildCompositeDataForThrowable(Throwable throwable) throws OpenDataException {
			Map<String, Object> items = new HashMap<>();
			if (throwable == null) {
				items.put(THROWABLE_TYPE_KEY, "");
				items.put(THROWABLE_MESSAGE_KEY, "");
				items.put(THROWABLE_CAUSE_KEY, "");
				items.put(THROWABLE_STACK_TRACE_KEY, new String[0]);
			} else {
				items.put(THROWABLE_TYPE_KEY, throwable.getClass().getName());
				items.put(THROWABLE_MESSAGE_KEY, throwable.getMessage());
				Throwable cause = throwable.getCause();
				items.put(THROWABLE_CAUSE_KEY, cause != null ? cause.getClass().getName() : "");
				String[] stackTrace = MBeanFormat.formatException(throwable);
				items.put(THROWABLE_STACK_TRACE_KEY, stackTrace);
			}
			return new CompositeDataSupport(getCompositeTypeOfThrowable(), items);
		}

		@Override
		public void setAttribute(Attribute attribute) throws AttributeNotFoundException, InvalidAttributeValueException,
				MBeanException, ReflectionException {
			MBeanAttributeInfo attrInfo = nameToSimpleAttribute.get(attribute.getName());
			if (attrInfo == null) {
				throw new AttributeNotFoundException(format("There is no attribute with name \"%s\"", attribute));
			}
			if (!attrInfo.isWritable()) {
				throw new AttributeNotFoundException(format("Attribute with name \"%s\" is not writable", attribute));
			}
			for (JmxMonitorableWrapper wrapper : wrappers) {
				try {
					wrapper.setSimpleAttribute(attribute.getName(), attribute.getValue());
				} catch (InvocationTargetException | IllegalAccessException e) {
					throw new ReflectionException(e,
							format("Cannot set value \"%s\" to attribute with name \"%s\"",
									attribute.getName(), attribute.getValue().toString())
					);
				}
			}
		}

		@Override
		public AttributeList getAttributes(String[] attributes) {
			checkArgument(attributes != null);

			AttributeList attrList = new AttributeList();
			for (String attrName : attributes) {
				try {
					attrList.add(new Attribute(attrName, getAttribute(attrName)));
				} catch (AttributeNotFoundException | MBeanException | ReflectionException e) {
					// TODO(vmykhalko): maybe use another exception handling policy (e.g. logging) ?
					e.printStackTrace();
				}
			}
			return attrList;
		}

		@Override
		public AttributeList setAttributes(AttributeList attributes) {
			AttributeList resultList = new AttributeList();
			for (int i = 0; i < attributes.size(); i++) {
				Attribute attribute = (Attribute) attributes.get(i);
				try {
					setAttribute(attribute);
					resultList.add(new Attribute(attribute.getName(), attribute.getValue()));
				} catch (AttributeNotFoundException | InvalidAttributeValueException
						| MBeanException | ReflectionException e) {
					// TODO(vmykhalko): maybe use another exception handling policy (e.g. logging) ?
					e.printStackTrace();
				}
			}
			return resultList;
		}

		@Override
		public Object invoke(final String actionName, final Object[] params, final String[] signature)
				throws MBeanException, ReflectionException {
			if (execIfActionIsForRefreshControl(actionName, params, signature)) {
				return null;
			}

			final CountDownLatch latch = new CountDownLatch(wrappers.size());
			final AtomicReference<Exception> exceptionReference = new AtomicReference<>();
			for (final JmxMonitorableWrapper wrapper : wrappers) {
				Executor executor = wrapper.getExecutor();
				executor.execute(new Runnable() {
					@Override
					public void run() {
						try {
							wrapper.invoke(actionName, params, signature);
							latch.countDown();
						} catch (Exception e) {
							exceptionReference.set(e);
							latch.countDown();
						}
					}
				});
			}

			try {
				latch.await();
			} catch (InterruptedException e) {
				throw new MBeanException(e);
			}

			Exception exception = exceptionReference.get();
			if (exception != null) {
				throw new MBeanException(exception);
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
				checkState(refreshEnabled());
				refreshPeriod = (double) params[0];
				return true;
			} else if (actionName.equals(SET_SMOOTHING_WINDOW_OP_NAME) && singleArgDouble) {
				checkState(refreshEnabled());
				smoothingWindow = (double) params[0];
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
		private final ConcurrentJmxMBean monitorable;
		private final Map<String, Method> jmxStatsGetters;
		private final List<? extends JmxStats<?>> jmxStatsList;
		private final Map<String, Method> simpleAttributeGetters;
		private final Map<String, Method> simpleAttributeSetters;
		private final Map<String, Method> listAttributeGetters;
		private final Map<String, Method> arrayAttributeGetters;
		private final Map<String, Method> throwableAttributeGetters;
		private final Map<OperationKey, Method> operationKeyToMethod;

		public JmxMonitorableWrapper(ConcurrentJmxMBean monitorable, Map<String, Method> jmxStatsGetters,
		                             List<? extends JmxStats<?>> jmxStatsList,
		                             Map<String, Method> simpleAttributeGetters,
		                             Map<String, Method> simpleAttributeSetters,
		                             Map<String, Method> listAttributeGetters,
		                             Map<String, Method> arrayAttributeGetters,
		                             Map<String, Method> throwableAttributeGetters,
		                             Map<OperationKey, Method> operationKeyToMethod) {
			this.monitorable = monitorable;
			// TODO (vmykhalko): maybe do not use getters of jmx stats ?
			this.jmxStatsGetters = jmxStatsGetters;
			this.jmxStatsList = jmxStatsList;
			this.simpleAttributeGetters = simpleAttributeGetters;
			this.simpleAttributeSetters = simpleAttributeSetters;
			this.listAttributeGetters = listAttributeGetters;
			this.arrayAttributeGetters = arrayAttributeGetters;
			this.throwableAttributeGetters = throwableAttributeGetters;
			this.operationKeyToMethod = operationKeyToMethod;
		}

		public JmxStats<?> getJmxStats(String jmxStatsName) throws AttributeNotFoundException, ReflectionException {
			Method jmxStatsGetter = jmxStatsGetters.get(jmxStatsName);
			if (jmxStatsGetter == null) {
				throw new AttributeNotFoundException();
			}
			try {
				return (JmxStats<?>) jmxStatsGetter.invoke(monitorable);
			} catch (IllegalAccessException | InvocationTargetException e) {
				throw new ReflectionException(e);
			}
		}

		public Executor getExecutor() {
			return monitorable.getJmxExecutor();
		}

		public List<? extends JmxStats<?>> getAllJmxStats() {
			return jmxStatsList;
		}

		public Object getSimpleAttributeValue(String attrName) throws AttributeNotFoundException, ReflectionException {
			Method simpleAttributeGetter = simpleAttributeGetters.get(attrName);
			if (simpleAttributeGetter == null) {
				throw new AttributeNotFoundException();
			}
			try {
				return simpleAttributeGetter.invoke(monitorable);
			} catch (IllegalAccessException | InvocationTargetException e) {
				throw new ReflectionException(e);
			}
		}

		public List<?> getListAttributeValue(String attrName) throws AttributeNotFoundException, ReflectionException {
			Method listAttributeGetter = listAttributeGetters.get(attrName);
			if (listAttributeGetter == null) {
				throw new AttributeNotFoundException();
			}
			try {
				return (List<?>) listAttributeGetter.invoke(monitorable);
			} catch (IllegalAccessException | InvocationTargetException e) {
				throw new ReflectionException(e);
			}
		}

		public Object[] getArrayAttributeValue(String attrName) throws AttributeNotFoundException, ReflectionException {
			Method arrayAttributeGetter = arrayAttributeGetters.get(attrName);
			if (arrayAttributeGetter == null) {
				throw new AttributeNotFoundException();
			}
			try {
				return (Object[]) arrayAttributeGetter.invoke(monitorable);
			} catch (IllegalAccessException | InvocationTargetException e) {
				throw new ReflectionException(e);
			}
		}

		public Throwable getThrowableAttributeValue(String attrName)
				throws AttributeNotFoundException, ReflectionException {
			Method throwableAttributeGetter = throwableAttributeGetters.get(attrName);
			if (throwableAttributeGetter == null) {
				throw new AttributeNotFoundException();
			}
			try {
				return (Throwable) throwableAttributeGetter.invoke(monitorable);
			} catch (IllegalAccessException | InvocationTargetException e) {
				throw new ReflectionException(e);
			}
		}

		public void setSimpleAttribute(String attrName, Object value)
				throws InvocationTargetException, IllegalAccessException {
			Method setter = simpleAttributeSetters.get(attrName);
			setter.invoke(monitorable, value);
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
	}
}
