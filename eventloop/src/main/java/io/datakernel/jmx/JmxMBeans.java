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

import io.datakernel.time.CurrentTimeProvider;
import io.datakernel.time.CurrentTimeProviderSystem;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.management.*;
import javax.management.openmbean.*;
import java.lang.annotation.Annotation;
import java.lang.reflect.Array;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.*;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static io.datakernel.jmx.RefreshTaskPerPool.ExecutorAndStatsList;
import static io.datakernel.jmx.Utils.*;
import static io.datakernel.util.Preconditions.*;
import static java.lang.String.format;
import static java.util.Arrays.asList;

public final class JmxMBeans implements DynamicMBeanFactory {
	private static final Logger logger = LoggerFactory.getLogger(JmxMBeans.class);

	private static final String ATTRIBUTE_NAME_FORMAT = "%s_%s";
	private static final String ATTRIBUTE_DEFAULT_DESCRIPTION = "";

	private static final Timer TIMER = new Timer(true);
	private static final String REFRESH_PERIOD_ATTRIBUTE_NAME = "_refreshPeriod";
	private static final String SMOOTHING_WINDOW_ATTRIBUTE_NAME = "_smoothingWindow";
	private static final String SET_REFRESH_PERIOD_OP_NAME = "_setRefreshPeriod";
	private static final String SET_SMOOTHING_WINDOW_OP_NAME = "_setSmoothingWindow";
	private static final String SET_REFRESH_PERIOD_PARAMETER_NAME = "period";
	private static final String SET_SMOOTHING_WINDOW_PARAMETER_NAME = "window";
	public static final double DEFAULT_REFRESH_PERIOD = 0.2;
	public static final double DEFAULT_SMOOTHING_WINDOW = 10.0;

	private static final String DEFAULT_COMPOSITE_DATA_NAME = "CompositeData";

	private static final Exception EXCEPTION = new Exception();
	private static final Exception AGGREGATION_EXCEPTION = new AggregationException("");

	private static final String NO_NAME_ATTR_KEY = "~noNameAttr~";

	// CompositeData of Throwable
	private static final String THROWABLE_COMPOSITE_TYPE_NAME = "CompositeDataOfThrowable";
	private static final String THROWABLE_TYPE_KEY = "type";
	private static final String THROWABLE_MESSAGE_KEY = "message";
	private static final String THROWABLE_CAUSE_KEY = "cause";
	private static final String THROWABLE_STACK_TRACE_KEY = "stackTrace";
	private static CompositeType compositeTypeOfThrowable;

	private static final Exception SIMPLE_TYPE_AGGREGATION_EXCEPTION =
			new Exception("Attribute values in pool instances are different");
	private static final Exception THROWABLE_AGGREGATION_EXCEPTION =
			new Exception("Throwables in pool instances are different");

	public static final long SNAPSHOT_UPDATE_DEFAULT_PERIOD = 200L; // milliseconds
	private static final CurrentTimeProvider TIME_PROVIDER = CurrentTimeProviderSystem.instance();
	private final long snapshotUpdatePeriod;

	private JmxMBeans(long snapshotUpdatePeriod) {
		this.snapshotUpdatePeriod = snapshotUpdatePeriod;
	}

	public static JmxMBeans factory() {
		return new JmxMBeans(SNAPSHOT_UPDATE_DEFAULT_PERIOD);
	}

	public static JmxMBeans factory(long snapshotUpdatePeriod) {
		return new JmxMBeans(snapshotUpdatePeriod);
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

		DynamicMBeanAggregator mbean = new DynamicMBeanAggregator(mBeanInfo, wrappers, nameToJmxStatsType,
				nameToSimpleAttribute, listAttributeNames, arrayAttributeNames, throwableAttributeNames,
				enableRefresh, snapshotUpdatePeriod);

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
				boolean writable = doesSetterForAttributeExist(monitorable.getClass(), name, method.getReturnType());
				if (writable) {
					writableAttributes.add(new SimpleAttribute(name, method.getReturnType()));
				}
			}
		}
		return writableAttributes;
	}

	private static Map<String, MBeanAttributeInfo> fetchNameToSimpleAttribute(Object monitorable) {
		List<MBeanAttributeInfo> simpleAttrs = fetchSimpleAttributesInfo(monitorable.getClass());
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

//			SortedMap<String, TypeAndValue> statsAttributes = stats.getAttributes();
			// TODO(vmykhalko): fix this
			SortedMap<String, TypeAndValue> statsAttributes = new TreeMap<>();

			for (String statsAttributeName : statsAttributes.keySet()) {
				TypeAndValue typeAndValue = statsAttributes.get(statsAttributeName);
				String attrName = format(ATTRIBUTE_NAME_FORMAT, statsName, statsAttributeName);
				String attrType = typeAndValue.getType().getTypeName();
				MBeanAttributeInfo attributeInfo =
						new MBeanAttributeInfo(attrName, attrType, ATTRIBUTE_DEFAULT_DESCRIPTION, true, false, false);
				attributes.add(attributeInfo);
			}
		}

		List<MBeanAttributeInfo> simpleAttrs = fetchSimpleAttributesInfo(monitorable.getClass());
		attributes.addAll(simpleAttrs);

		List<MBeanAttributeInfo> listAttrs = fetchListAttributesInfo(monitorable.getClass());
		attributes.addAll(listAttrs);

		List<MBeanAttributeInfo> arrayAttrs = fetchArrayAttributesInfo(monitorable.getClass());
		attributes.addAll(arrayAttrs);

		List<MBeanAttributeInfo> exceptionAttrs = fetchThrowableAttributesInfo(monitorable.getClass());
		attributes.addAll(exceptionAttrs);

		List<MBeanAttributeInfo> pojoAttrs = fetchPojoAttributesInfo(monitorable.getClass());
		attributes.addAll(pojoAttrs);

		List<MBeanAttributeInfo> mapAttrs = fetchMapAttributesInfo(monitorable.getClass());
		attributes.addAll(mapAttrs);

		if (enableRefresh) {
			addAttributesForRefreshControl(attributes);
		}

		return attributes.toArray(new MBeanAttributeInfo[attributes.size()]);
	}

	/**
	 * Simple attributes means that attribute's type is of one of the following: boolean, int, long, double, String
	 */
	private static List<MBeanAttributeInfo> fetchSimpleAttributesInfo(Class<?> clazz) {
		List<MBeanAttributeInfo> attrList = new ArrayList<>();
		Method[] methods = clazz.getMethods();
		for (Method method : methods) {
			if (method.isAnnotationPresent(JmxAttribute.class) && isGetterOfSimpleType(method)) {
				String name = extractFieldNameFromGetter(method);
				boolean writable = doesSetterForAttributeExist(clazz, name, method.getReturnType());
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
	private static List<MBeanAttributeInfo> fetchListAttributesInfo(Class<?> clazz) {
		List<MBeanAttributeInfo> attrList = new ArrayList<>();
		Method[] methods = clazz.getMethods();
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

	private static List<MBeanAttributeInfo> fetchArrayAttributesInfo(Class<?> clazz) {
		List<MBeanAttributeInfo> attrList = new ArrayList<>();
		Method[] methods = clazz.getMethods();
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

	private static List<MBeanAttributeInfo> fetchMapAttributesInfo(Class<?> clazz) {
		List<MBeanAttributeInfo> attrList = new ArrayList<>();
		Method[] methods = clazz.getMethods();
		for (Method method : methods) {
			if (method.isAnnotationPresent(JmxAttribute.class) && isGetterOfMap(method)) {
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

	private static List<MBeanAttributeInfo> fetchThrowableAttributesInfo(Class<?> clazz) {
		List<MBeanAttributeInfo> attrList = new ArrayList<>();
		Method[] methods = clazz.getMethods();
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

	private static List<MBeanAttributeInfo> fetchPojoAttributesInfo(Class<?> clazz) {
		List<MBeanAttributeInfo> attrList = new ArrayList<>();
		Method[] methods = clazz.getMethods();
		for (Method method : methods) {
			if (method.isAnnotationPresent(JmxAttribute.class) && isGetterOfPojo(method)) {
				String currentPojoName = extractFieldNameFromGetter(method);
				Class<?> currentPojoClass = method.getReturnType();
				List<MBeanAttributeInfo> simpleAttrs = fetchSimpleAttributesInfo(currentPojoClass);
				for (MBeanAttributeInfo simpleAttr : simpleAttrs) {
					attrList.add(addPrefixToAttrName(simpleAttr, currentPojoName));
				}

				List<MBeanAttributeInfo> listAttrs = fetchListAttributesInfo(currentPojoClass);
				for (MBeanAttributeInfo listAttr : listAttrs) {
					attrList.add(addPrefixToAttrName(listAttr, currentPojoName));
				}

				List<MBeanAttributeInfo> arrayAttrs = fetchArrayAttributesInfo(currentPojoClass);
				for (MBeanAttributeInfo arrayAttr : arrayAttrs) {
					attrList.add(addPrefixToAttrName(arrayAttr, currentPojoName));
				}

				List<MBeanAttributeInfo> pojoAttrs = fetchPojoAttributesInfo(currentPojoClass);
				for (MBeanAttributeInfo pojoAttr : pojoAttrs) {
					attrList.add(addPrefixToAttrName(pojoAttr, currentPojoName));
				}

				List<MBeanAttributeInfo> throwableAttrs = fetchThrowableAttributesInfo(currentPojoClass);
				for (MBeanAttributeInfo throwableAttr : throwableAttrs) {
					attrList.add(addPrefixToAttrName(throwableAttr, currentPojoName));
				}
			}
		}
		return attrList;
	}

	private static MBeanAttributeInfo addPrefixToAttrName(MBeanAttributeInfo attr, String prefix) {
		return new MBeanAttributeInfo(
				prefix + "_" + attr.getName(),
				attr.getType(),
				attr.getDescription(),
				attr.isReadable(),
				attr.isWritable(),
				attr.isIs());
	}

//	private static Map<String, >

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

	private static boolean doesSetterForAttributeExist(Class<?> clazz, String name, Class<?> type) {
		Method[] methods = clazz.getMethods();
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

		private volatile AttributesSnapshot lastAttributesSnapshot;

		private final long snapshotUpdatePeriod;

		// refresh
		private volatile double refreshPeriod;
		private volatile double smoothingWindow;
		private boolean refreshEnabled;

		public DynamicMBeanAggregator(MBeanInfo mBeanInfo, List<JmxMonitorableWrapper> wrappers,
		                              Map<String, Class<? extends JmxStats<?>>> nameToJmxStatsType,
		                              Map<String, MBeanAttributeInfo> nameToSimpleAttribute,
		                              Set<String> listAttributes, Set<String> arrayAttributes,
		                              Set<String> exceptionAttributes, boolean enableRefresh,
		                              long snapshotUpdatePeriod) {
			this.mBeanInfo = mBeanInfo;
			this.wrappers = wrappers;
			this.nameToJmxStatsType = nameToJmxStatsType;
			this.nameToSimpleAttribute = nameToSimpleAttribute;
			this.listAttributes = listAttributes;
			this.arrayAttributes = arrayAttributes;
			this.exceptionAttributes = exceptionAttributes;
			this.refreshEnabled = enableRefresh;
			this.snapshotUpdatePeriod = snapshotUpdatePeriod;
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

			if (lastAttributesSnapshot == null) {
				lastAttributesSnapshot = createFreshAttributesSnapshot();
			}

			long timePassedAfterLastSnapshotUpdate =
					TIME_PROVIDER.currentTimeMillis() - lastAttributesSnapshot.getTimestamp();

			if (timePassedAfterLastSnapshotUpdate >= snapshotUpdatePeriod) {
				lastAttributesSnapshot = createFreshAttributesSnapshot();
			}

			if (!lastAttributesSnapshot.containsAttribute(attribute)) {
				throw new AttributeNotFoundException(format("Error. Attribute with name \"%s\" do not exist " +
						"or error happened during fetching (or aggregating) its value. " +
						"Take a look to appropriate logger for details", attribute));
			}

			Object attrValue = lastAttributesSnapshot.getAttribute(attribute);

			if (attrValue instanceof Exception) {
				Exception attrException = (Exception) attrValue;
				throw new MBeanException(EXCEPTION,
						format("Exception with type \"%s\" and message \"%s\" occured during fetching attribute",
								attrException.getClass().getName(), attrException.getMessage()));
			}

			return attrValue;
		}

		private boolean refreshEnabled() {
			return refreshEnabled;
		}

		@SuppressWarnings("unchecked")
		private Map<String, Object> aggregateAllJmxStatsAttributes() {
			Map<String, Object> nameToAttribute = new HashMap<>();
			for (final String jmxStatsName : nameToJmxStatsType.keySet()) {
				Class<? extends JmxStats<?>> jmxStatsType = nameToJmxStatsType.get(jmxStatsName);
				// wildcard is removed intentionally, types must be same
				JmxStats statsAccumulator;
				try {
					statsAccumulator = jmxStatsType.newInstance();
				} catch (InstantiationException | IllegalAccessException e) {
					String errorMsg = "All JmxStats Implementations must have public constructor without parameters";
					logger.error(errorMsg, e);
					continue;
				}

				try {
					for (final JmxMonitorableWrapper wrapper : wrappers) {
						statsAccumulator.add(wrapper.getJmxStats(jmxStatsName));
					}
				} catch (IllegalArgumentException | InvocationTargetException | IllegalAccessException e) {
					String errorMsg =
							format("Cannot fetch JmxStats with name \"%s\" during JmxStats accumulation", jmxStatsName);
					logger.error(errorMsg, e);
					continue;
				}

//				Map<String, TypeAndValue> jmxStatsAttributes = statsAccumulator.getAttributes();
				// TODO(vmykhalko): fix this
				Map<String, TypeAndValue> jmxStatsAttributes = new HashMap<>();

				for (String subAttrName : jmxStatsAttributes.keySet()) {
					String attrName = jmxStatsName + "_" + subAttrName;
					nameToAttribute.put(attrName, jmxStatsAttributes.get(subAttrName).getValue());
				}
			}
			return nameToAttribute;
		}

		private Map<String, Object> aggregateAllSimpleTypeAttributes() {
			Map<String, Object> nameToAttribute = new HashMap<>();
			for (String attrName : nameToSimpleAttribute.keySet()) {
				try {
					JmxMonitorableWrapper first = wrappers.get(0);
					Object attrValue = first.getSimpleAttributeValue(attrName);
					boolean accumulationFailed = false;
					for (int i = 1; i < wrappers.size(); i++) {
						Object currentAttrValue = wrappers.get(i).getSimpleAttributeValue(attrName);
						if (!Objects.equals(attrValue, currentAttrValue)) {
							accumulationFailed = true;
							break;
						}
					}
					if (!accumulationFailed) {
						nameToAttribute.put(attrName, attrValue);
					} else {
						nameToAttribute.put(attrName, AGGREGATION_EXCEPTION);
					}
				} catch (IllegalArgumentException | IllegalAccessException | InvocationTargetException e) {
					String errorMsg =
							format("Cannot fetch simple-type attribute with name \"%s\"", attrName);
					// TODO (vmykhalko): is it true to use both logging and throwing RuntimeException ?
					logger.error(errorMsg, e);
					throw new RuntimeException(errorMsg, e);
				}
			}
			return nameToAttribute;
		}

		private Map<String, String[]> aggregateAllListAttributes() {
			Map<String, String[]> nameToAttribute = new HashMap<>();
			for (String listAttrName : listAttributes) {
				try {
					List<String> allItems = new ArrayList<>();
					for (JmxMonitorableWrapper wrapper : wrappers) {
						List<?> currentList = wrapper.getListAttributeValue(listAttrName);
						if (currentList != null) {
							for (Object o : currentList) {
								String item = o != null ? o.toString() : "null";
								allItems.add(item);
							}
						}
					}
					nameToAttribute.put(listAttrName, allItems.toArray(new String[allItems.size()]));
				} catch (IllegalArgumentException | IllegalAccessException | InvocationTargetException e) {
					String errorMsg =
							format("Cannot fetch list attribute with name \"%s\"", listAttrName);
					logger.error(errorMsg, e);
				}
			}
			return nameToAttribute;
		}

		// TODO(vmykhalko): refactor this method - it resembles aggregateAllListAttributes()
		private Map<String, String[]> aggregateAllArrayAttributes() {
			Map<String, String[]> nameToAttribute = new HashMap<>();
			for (String arrayAttrName : arrayAttributes) {
				try {
					List<String> allItems = new ArrayList<>();
					for (JmxMonitorableWrapper wrapper : wrappers) {
						Object[] currentArray = wrapper.getArrayAttributeValue(arrayAttrName);
						if (currentArray != null) {
							for (Object o : currentArray) {
								String item = o != null ? o.toString() : "null";
								allItems.add(item);
							}
						}
					}
					nameToAttribute.put(arrayAttrName, allItems.toArray(new String[allItems.size()]));
				} catch (IllegalArgumentException | IllegalAccessException | InvocationTargetException e) {
					String errorMsg =
							format("Cannot fetch array attribute with name \"%s\"", arrayAttrName);
					logger.error(errorMsg, e);
				}
			}
			return nameToAttribute;
		}

		private Map<String, Object> aggregateAllExceptionAttributes() {
			Map<String, Object> nameToAttribute = new HashMap<>();
			for (String exceptionAttrName : exceptionAttributes) {
				try {
					JmxMonitorableWrapper first = wrappers.get(0);
					Throwable throwable = first.getThrowableAttributeValue(exceptionAttrName);
					for (int i = 1; i < wrappers.size(); i++) {
						Throwable currentThrowable = wrappers.get(i).getThrowableAttributeValue(exceptionAttrName);
						if (!Objects.equals(throwable, currentThrowable)) {
							throw new AggregationException();
						}
					}
					nameToAttribute.put(exceptionAttrName, buildCompositeDataForThrowable(throwable));
				} catch (AggregationException e) {
					nameToAttribute.put(exceptionAttrName, THROWABLE_AGGREGATION_EXCEPTION);
				} catch (IllegalArgumentException | IllegalAccessException | InvocationTargetException e) {
					String errorMsg =
							format("Cannot fetch Throwable attribute with name \"%s\"", exceptionAttrName);
					logger.error(errorMsg, e);
				} catch (OpenDataException e) {
					String errorMsg = format("Cannot create CompositeData for Throwable attribute " +
							"with name \"%s\"", exceptionAttrName);
					logger.error(errorMsg, e);
				}
			}
			return nameToAttribute;
		}

		private static CompositeData buildCompositeDataForThrowable(Throwable throwable) throws OpenDataException {
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

		private static CompositeData buildCompositeDataForPojo(Object pojo) throws OpenDataException {
			// TODO(vmykhalko): implement
			Map<String, Object> attrs = fetchAndAccumulateAllAttributes(asList(pojo));
			CompositeDataBuilder.Builder builder = CompositeDataBuilder.builder(DEFAULT_COMPOSITE_DATA_NAME);
			for (String attrName : attrs.keySet()) {
				Object attrValue = attrs.get(attrName);
				if (attrValue == null) {
					builder.add(attrName, SimpleType.STRING, "null");
				} else {
					builder.add(attrName, determineOpenTypeOfInstance(attrValue), attrValue);
				}
			}
			return builder.build();
		}

		private static CompositeData buildCompositeDataFromMap(Map<String, ?> map) {
			try {
				CompositeDataBuilder.Builder builder = CompositeDataBuilder.builder("CompositeData");
				for (String key : map.keySet()) {
					Object value = map.get(key);
					builder.add(key, determineOpenTypeOfInstance(value), value);
				}
				return builder.build();
			} catch (OpenDataException e) {
				throw new RuntimeException(e);
			}
		}

		@Override
		public void setAttribute(final Attribute attribute) throws AttributeNotFoundException, InvalidAttributeValueException,
				MBeanException, ReflectionException {
			MBeanAttributeInfo attrInfo = nameToSimpleAttribute.get(attribute.getName());
			if (attrInfo == null) {
				throw new AttributeNotFoundException(format("There is no attribute with name \"%s\"", attribute));
			}
			if (!attrInfo.isWritable()) {
				throw new AttributeNotFoundException(format("Attribute with name \"%s\" is not writable", attribute));
			}

			final CountDownLatch latch = new CountDownLatch(wrappers.size());
			final AtomicReference<Exception> exceptionReference = new AtomicReference<>();
			for (final JmxMonitorableWrapper wrapper : wrappers) {
				Executor executor = wrapper.getExecutor();
				executor.execute(new Runnable() {
					@Override
					public void run() {
						try {
							wrapper.setSimpleAttribute(attribute.getName(), attribute.getValue());
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

		private AttributesSnapshot createFreshAttributesSnapshot() {
			List<Object> monitorableInstances = new ArrayList<>();
			for (JmxMonitorableWrapper wrapper : wrappers) {
				monitorableInstances.add(wrapper.getMonitorable());
			}

			return new AttributesSnapshot(
					TIME_PROVIDER.currentTimeMillis(),
					fetchAndAccumulateAllAttributes(monitorableInstances)
			);
		}

		private static Map<String, Object> fetchAndAccumulateAllAttributes(List<?> objects) {
			Map<String, Object> nameToAttribute = new HashMap<>();
			nameToAttribute.putAll(fetchAndAccumulateSimpleTypeAttrs(objects));
			nameToAttribute.putAll(fetchAndAccumulatePojoAttrs(objects));
			nameToAttribute.putAll(fetchAndAccumulateListAttrs(objects));
			nameToAttribute.putAll(fetchAndAccumulateArrayAttrs(objects));
			nameToAttribute.putAll(fetchAndAccumulateThrowableAttrs(objects));
			nameToAttribute.putAll(fetchAndAccumulateJmxStatsAttrs(objects));
			nameToAttribute.putAll(fetchAndAccumulateMapAttrs(objects));
			return nameToAttribute;
		}

		private static Map<String, TypeAndValue> fetchAndAccumulateSimpleTypeAttrs(List<?> objects) {
			// TODO (vmykhalko): add preconditions

			Map<String, TypeAndValue> nameToAccumulatedAttr = new HashMap<>();
			Object first = objects.get(0);
			List<Method> getters = fetchSimpleTypeAttrGetters(first.getClass());
			for (Method getter : getters) {
				String attrName = extractFieldNameFromGetter(getter);
				List<Object> values = new ArrayList<>();
				for (Object object : objects) {
					Object currentValue;
					try {
						currentValue = getter.invoke(object);
						values.add(currentValue);
					} catch (IllegalAccessException | InvocationTargetException e) {
						logger.error(format("Cannot fetch attribute with name \"%s\" during accumulation", attrName), e);
						throw new RuntimeException(e);
					}

				}
				nameToAccumulatedAttr.putAll(
						addPrefixToKeys(accumulateSimpleTypeAttrs(values, getter.getReturnType()), attrName));
			}
			return nameToAccumulatedAttr;
		}

		private static Map<String, TypeAndValue> accumulateSimpleTypeAttrs(List<?> attrs, Class<?> simpleTypeClass) {
			JmxAccumulator accumulator = accumulatorForSimpleType(simpleTypeClass);
			for (Object attr : attrs) {
				accumulator.add(attr);
			}
			return fetchAttrNameToValueFromAccumulator(accumulator);
		}

		private static JmxAccumulator accumulatorForSimpleType(Class<?> returnType) {
			if (returnType == boolean.class || returnType == Boolean.class) {
				return JmxAccumulators.defaultBooleanAccumulator();
			} else if (returnType == byte.class || returnType == Byte.class) {
				return JmxAccumulators.defaultByteAccumulator();
			} else if (returnType == short.class || returnType == Short.class) {
				return JmxAccumulators.defaultShortAccumulator();
			} else if (returnType == char.class || returnType == Character.class) {
				return JmxAccumulators.defaultCharacterAccumulator();
			} else if (returnType == int.class || returnType == Integer.class) {
				return JmxAccumulators.defaultIntegerAccumulator();
			} else if (returnType == long.class || returnType == Long.class) {
				return JmxAccumulators.defaultLongAccumulator();
			} else if (returnType == float.class || returnType == Float.class) {
				return JmxAccumulators.defaultFloatAccumulator();
			} else if (returnType == double.class || returnType == Double.class) {
				return JmxAccumulators.defaultDoubleAccumulator();
			} else if (returnType == String.class) {
				return JmxAccumulators.defaultStringAccumulator();
			}
			throw new IllegalArgumentException("There is no accumulator for " + returnType.getName());
		}

		private static SimpleType<?> simpleTypeOf(Class<?> clazz) throws IllegalArgumentException {
			if (clazz == boolean.class || clazz == Boolean.class) {
				return SimpleType.BOOLEAN;
			} else if (clazz == byte.class || clazz == Byte.class) {
				return SimpleType.BYTE;
			} else if (clazz == short.class || clazz == Short.class) {
				return SimpleType.SHORT;
			} else if (clazz == char.class || clazz == Character.class) {
				return SimpleType.CHARACTER;
			} else if (clazz == int.class || clazz == Integer.class) {
				return SimpleType.INTEGER;
			} else if (clazz == long.class || clazz == Long.class) {
				return SimpleType.LONG;
			} else if (clazz == float.class || clazz == Float.class) {
				return SimpleType.FLOAT;
			} else if (clazz == double.class || clazz == Double.class) {
				return SimpleType.DOUBLE;
			} else if (clazz == String.class) {
				return SimpleType.STRING;
			} else {
				throw new IllegalArgumentException("There is no SimpleType for " + clazz.getName());
			}
		}

		private static boolean isSimpleType(Class<?> clazz) {
			return isPrimitiveType(clazz) || isPrimitiveTypeWrapper(clazz) || isString(clazz);
		}

		private static List<Method> fetchSimpleTypeAttrGetters(Class<?> clazz) {
			List<Method> getters = new ArrayList<>();
			for (Method method : clazz.getMethods()) {
				if (isGetterOfSimpleType(method) && method.isAnnotationPresent(JmxAttribute.class)) {
					getters.add(method);
				}
			}
			return getters;
		}

		private static List<Method> fetchPojoAttrGetters(Class<?> clazz) {
			List<Method> getters = new ArrayList<>();
			for (Method method : clazz.getMethods()) {
				if (isGetterOfPojo(method) && method.isAnnotationPresent(JmxAttribute.class)) {
					getters.add(method);
				}
			}
			return getters;
		}

		private static List<Method> fetchListAttrGetters(Class<?> clazz) {
			List<Method> getters = new ArrayList<>();
			for (Method method : clazz.getMethods()) {
				if (isGetterOfList(method) && method.isAnnotationPresent(JmxAttribute.class)) {
					getters.add(method);
				}
			}
			return getters;
		}

		private static List<Method> fetchArrayAttrGetters(Class<?> clazz) {
			List<Method> getters = new ArrayList<>();
			for (Method method : clazz.getMethods()) {
				if (isGetterOfArray(method) && method.isAnnotationPresent(JmxAttribute.class)) {
					getters.add(method);
				}
			}
			return getters;
		}

		private static List<Method> fetchMapAttrGetters(Class<?> clazz) {
			List<Method> getters = new ArrayList<>();
			for (Method method : clazz.getMethods()) {
				if (isGetterOfMap(method) && method.isAnnotationPresent(JmxAttribute.class)) {
					getters.add(method);
				}
			}
			return getters;
		}

		private static List<Method> fetchThrowableAttrGetters(Class<?> clazz) {
			List<Method> getters = new ArrayList<>();
			for (Method method : clazz.getMethods()) {
				if (isGetterOfThrowable(method) && method.isAnnotationPresent(JmxAttribute.class)) {
					getters.add(method);
				}
			}
			return getters;
		}

		private static List<Method> fetchJmxStatsAttrGetters(Class<?> clazz) {
			List<Method> getters = new ArrayList<>();
			for (Method method : clazz.getMethods()) {
				if (isGetterOfJmxStats(method) && method.isAnnotationPresent(JmxAttribute.class)) {
					getters.add(method);
				}
			}
			return getters;
		}

		/*
		 * Collects attributes and convert them to be jmx compatible if necessary
		 */
		private static Map<String, TypeAndValue> fetchAttrNameToValueFromAccumulator(JmxAccumulator accumulator) {
			checkNotNull(accumulator);

			try {
				List<Method> attributeGetters = fetchAllAttributeGetters(accumulator);
				Map<String, TypeAndValue> attrNameToTypeAndValue = new HashMap<>();
				if (attributeGetters.size() == 1) {
					Method getter = attributeGetters.get(0);
					JmxAttribute attrAnnotation = getter.getAnnotation(JmxAttribute.class);
					String attrName = attrAnnotation.skipName() ? NO_NAME_ATTR_KEY : extractFieldNameFromGetter(getter);
					attrNameToTypeAndValue.put(attrName, fetchTypeAndValueFromAccumulatorGetter(getter, accumulator));
				} else {
					for (Method getter : attributeGetters) {
						String attrName = extractFieldNameFromGetter(getter);
						attrNameToTypeAndValue.put(attrName, fetchTypeAndValueFromAccumulatorGetter(getter, accumulator));
					}
				}
				return attrNameToTypeAndValue;
			} catch (IllegalAccessException | OpenDataException | InvocationTargetException e) {
				throw new RuntimeException(e);
			}
		}

		private static TypeAndValue fetchTypeAndValueFromAccumulatorGetter(Method getter, JmxAccumulator<?> accumulator)
				throws InvocationTargetException, IllegalAccessException, OpenDataException {
			Object value = getter.invoke(accumulator);
			Class<?> returnType = getter.getReturnType();
			OpenType<?> attrType;
			Object compatibleValue;
			if (isSimpleType(returnType)) {
				attrType = simpleTypeOf(returnType);
				compatibleValue = value;
			} else if (isThrowable(returnType)) {
				CompositeData compositeData = buildCompositeDataForThrowable((Throwable) value);
				attrType = compositeData.getCompositeType();
				compatibleValue = compositeData;
			} else if (isArray(returnType)) {
				Class<?> arrayElementType = returnType.getComponentType();
				if (isArray(arrayElementType)) {
					throw new RuntimeException(
							"There is no support for multi-dimensional arrays in JmxAccumulator");
				}
				if (!isSimpleType(arrayElementType)) {
					throw new RuntimeException("Only arrays with Strings, primitives or their wrappers " +
							"are supported in JmxAccumulator");
				}
				attrType = new ArrayType<>(1, simpleTypeOf(arrayElementType));
				compatibleValue = value;
			} else {
				throw new RuntimeException("Only primitives, wrappers, Strings, " +
						"Throwables and arrays are supported in JmxAccumulator");
			}
			return new TypeAndValue(attrType, compatibleValue);
		}

		private static Map<String, Object> fetchAttrNameToValueFromAccumulatorIgnoreSkipName(JmxAccumulator accumulator) {
			checkNotNull(accumulator);

			List<Method> attributeGetters = fetchAllAttributeGetters(accumulator);
			Map<String, Object> attrNameToValue = new HashMap<>();
			for (Method getter : attributeGetters) {
				String attrName = extractFieldNameFromGetter(getter);
				try {
					Object value = getter.invoke(accumulator);
					attrNameToValue.put(attrName, ensureJmxCompatibility(value));
				} catch (IllegalAccessException e) {
					logger.error(format("Cannot fetch attribute with name \"%s\"", attrName), e);
					throw new RuntimeException(e);
				} catch (InvocationTargetException e) {
					if (e.getCause().getClass() == AggregationException.class) {
						attrNameToValue.put(attrName, null);
					} else {
						throw new RuntimeException(e);
					}
				}
			}
			return attrNameToValue;
		}

		private static List<Method> fetchAllAttributeGetters(Object object) {
			checkNotNull(object);

			List<Method> attributeGetters = new ArrayList<>();
			for (Method method : object.getClass().getMethods()) {
				if (isGetter(method) && method.isAnnotationPresent(JmxAttribute.class)) {
					attributeGetters.add(method);
				}
			}
			return attributeGetters;
		}

		private static TypeAndValue ensureJmxCompatibility(Object value) {
			checkNotNull(value);

			Class clazz = value.getClass();
			if (isPrimitiveType(clazz) || isPrimitiveTypeWrapper(clazz) || isString(clazz)) {
				return new TypeAndValue(simpleTypeOf(clazz), value);
			} else if (isList(clazz)) {
				return jmxCompatibleArrayOf((List<?>) value);
			} else if (isMap(clazz)) {
				return tabularDataOf((Map<?, ?> value))
			}else if (isThrowable(clazz)) {
				try {
					return buildCompositeDataForThrowable((Throwable) value);
				} catch (OpenDataException e) {
					throw new RuntimeException(e);
				}
			} else {
				try {
					return buildCompositeDataForPojo(value);
				} catch (OpenDataException e) {
					throw new RuntimeException(e);
				}
			}
		}

		private static boolean containsAtLeastOneNonEmptyElement(List<?> list) {
			return findFirstNotEmptyElementInList(list) != null;
		}

		private static boolean containsAtLeastOneNonEmptyElement(Map<?, ?> map) {
			return findFirstNotEmptyElementInMap(map) != null;
		}

		private static Object findFirstNotEmptyElementInList(List<?> list) {
			if (list == null) {
				return null;
			}
			for (Object element : list) {
				if (element != null) {
					if (element instanceof List) {
						return findFirstNotEmptyElementInList((List<?>) element) != null ? element : null;
					} else if (element instanceof Map) {
						return findFirstNotEmptyElementInMap((Map<?, ?>) element) != null ? element : null;
					} else {
						return element;
					}
				}
			}
			return null;
		}

		private static Object findFirstNotEmptyElementInMap(Map<?, ?> map) {
			if (map == null) {
				return null;
			}
			for (Map.Entry<?, ?> entry : map.entrySet()) {
				Object key = entry.getKey();
				Object value = entry.getValue();
				if (key != null && value != null) {
					if (value instanceof List) {
						return findFirstNotEmptyElementInList((List<?>) value) != null ? value : null;
					} else if (value instanceof Map) {
						return findFirstNotEmptyElementInMap((Map<?, ?>) value) != null ? value : null;
					} else {
						return value;
					}
				}
			}
			return null;
		}

		private static TypeAndValue jmxCompatibleArrayOf(List<?> list) {
			// TODO(vmykhalko): start here (17.02.2016)
			// TODO(vmykhalko): try to filter empty element in some way
			List<Object> outputList = new ArrayList<>();
			Object firstNonEmptyElement = findFirstNotEmptyElementInList(list);
			OpenType<?> openType = ensureJmxCompatibility(firstNonEmptyElement).getType();
			Class<?> listElementActualType = null;
			for (Object element : list) {
				if (element != null) {
					TypeAndValue jmxCompatibleElement = ensureJmxCompatibility(element);

//					if (jmxCompatibleElement != null) {
//						Class<?> currentElementType = jmxCompatibleElement.getClass();
//						if (listElementActualType == null) {
//							listElementActualType = currentElementType;
//						} else if (listElementActualType != currentElementType) {
//							String errorMsg = "Error. At least two list element types are different";
//							logger.error(errorMsg);
//							throw new RuntimeException(errorMsg);
//						}
//					}
//					outputList.add(jmxCompatibleElement);
				}
			}

			if (listElementActualType == null) {
				return null;
			}

			Object[] array = (Object[]) Array.newInstance(listElementActualType, outputList.size());
			for (int i = 0; i < outputList.size(); i++) {
				Object object = outputList.get(i);
				array[i] = object;
			}
			return array;
		}

		private static Map<String, TypeAndValue> fetchAndAccumulatePojoAttrs(List<?> objects) {
			// TODO (vmykhalko): add preconditions

			Map<String, TypeAndValue> nameToAccumulatedAttr = new HashMap<>();
			Object first = objects.get(0);
			List<Method> getters = fetchPojoAttrGetters(first.getClass());
			for (Method getter : getters) {
				String pojoName = extractFieldNameFromGetter(getter);
				List<Object> currentPojos = new ArrayList<>(objects.size());
				for (Object object : objects) {
					try {
						Object currentPojo = getter.invoke(object);
						currentPojos.add(currentPojo);
					} catch (IllegalAccessException | InvocationTargetException e) {
						logger.error(format("Cannot fetch pojo attribute with name \"%s\"", pojoName), e);
						throw new RuntimeException(e);
					}
				}

				nameToAccumulatedAttr.putAll(addPrefixToKeys(accumulatePojos(currentPojos), pojoName));
			}

			return nameToAccumulatedAttr;
		}

		private static Map<String, TypeAndValue> addPrefixToKeys(Map<String, TypeAndValue> map, String prefix) {
			Map<String, TypeAndValue> mapWithPrefixes = new HashMap<>(map.size());
			if (map.size() == 1 && map.containsKey(NO_NAME_ATTR_KEY)) {
				mapWithPrefixes.put(prefix, map.get(NO_NAME_ATTR_KEY));
			} else {
				for (String key : map.keySet()) {
					mapWithPrefixes.put(prefix + "_" + key, map.get(key));
				}
			}
			return mapWithPrefixes;
		}

		private static Map<String, TypeAndValue> accumulatePojos(List<Object> pojos) {
			Map<String, TypeAndValue> nameToAccumulatedAttr = new HashMap<>();

			nameToAccumulatedAttr.putAll(fetchAndAccumulateSimpleTypeAttrs(pojos));
			nameToAccumulatedAttr.putAll(fetchAndAccumulatePojoAttrs(pojos));
			nameToAccumulatedAttr.putAll(fetchAndAccumulateListAttrs(pojos));
			nameToAccumulatedAttr.putAll(fetchAndAccumulateThrowableAttrs(pojos));
			nameToAccumulatedAttr.putAll(fetchAndAccumulateJmxStatsAttrs(pojos));
			nameToAccumulatedAttr.putAll(fetchAndAccumulateMapAttrs(pojos));

			return nameToAccumulatedAttr;
		}

		private static Map<String, TypeAndValue> fetchAndAccumulateListAttrs(List<?> objects) {
			// TODO (vmykhalko): add preconditions
			checkArgument(objects.size() > 0);

			Map<String, Object> nameToAccumulatedAttr = new HashMap<>();
			Object first = objects.get(0);
			List<Method> getters = fetchListAttrGetters(first.getClass());
			for (Method getter : getters) {
				String attrName = extractFieldNameFromGetter(getter);
				List<Object> flattenedList = new ArrayList<>();
				for (Object object : objects) {
					try {
						List<?> currentList = (List<?>) getter.invoke(object);
						if (currentList != null && currentList.size() > 0) {
							flattenedList.addAll(currentList);
						}
					} catch (IllegalAccessException | InvocationTargetException e) {
						throw new RuntimeException(e);
					}

				}

				if (!containsAtLeastOneNonEmptyElement(flattenedList)) {
					// if all lists are empty we can't determine OpenType, so let's assume it's empty list of Strings
					TypeAndValue empty = new TypeAndValue(new ArrayType<>(1, SimpleType.STRING), new String[0]);
					nameToAccumulatedAttr.put(attrName, empty);
				} else {
					// TODO (vmykhalko): check for absence of empty map and lists here
					Object jmxCompatibleValue = jmxCompatibleArrayOf(flattenedList);
					OpenType<?> type = determineOpenTypeOfInstance(jmxCompatibleValue);

				}



				nameToAccumulatedAttr.putAll(extractAttrsFromAccumulator(attrName, accumulator));
			}
			return nameToAccumulatedAttr;
		}

//		private static OpenType<?>

		private static Map<String, Object> fetchAndAccumulateArrayAttrs(List<?> objects) {
			// TODO (vmykhalko): add preconditions

			Map<String, Object> nameToAccumulatedAttr = new HashMap<>();
			Object first = objects.get(0);
			List<Method> getters = fetchArrayAttrGetters(first.getClass());
			for (Method getter : getters) {
				String attrName = extractFieldNameFromGetter(getter);
				JmxAccumulator<Object[]> accumulator = JmxAccumulators.getArrayAccumulator();
				for (Object object : objects) {
					try {
						Object[] currentArray = (Object[]) getter.invoke(object);
						accumulator.add(currentArray);
					} catch (IllegalAccessException | InvocationTargetException e) {
						logger.error(
								format("Cannot fetch array attribute with name \"%s\" during accumulation", attrName),
								e);
						throw new RuntimeException(e);
					}

				}
				nameToAccumulatedAttr.putAll(extractAttrsFromAccumulator(attrName, accumulator));
			}
			return nameToAccumulatedAttr;
		}

		private static Map<String, Object> fetchAndAccumulateMapAttrs(List<?> objects) {
			// TODO (vmykhalko): add preconditions
			checkArgument(objects.size() > 0);

			Map<String, Object> nameToAccumulatedAttr = new HashMap<>();
			Object first = objects.get(0);
			List<Method> getters = fetchMapAttrGetters(first.getClass());
			for (Method getter : getters) {
				String attrName = extractFieldNameFromGetter(getter);
				Map<?, ?> firstNotEmptyMap = findFirstNotEmptyMap(objects, getter);
				Map<Object, List<Object>> groupedByKey = fetchMapsAndGroupEntriesByKey(objects, getter);
				Class<?> mapValueClass = checkNotNull(extractOneValueFromMap(firstNotEmptyMap)).getClass();

				if (JmxStats.class.isAssignableFrom(mapValueClass)) {
					List<CompositeData> compositeDataList = new ArrayList<>();
					for (Object key : groupedByKey.keySet()) {
						List<JmxStats<?>> jmxStatsList = new ArrayList<>();
						for (Object jmxStats : groupedByKey.get(key)) {
							jmxStatsList.add((JmxStats<?>) jmxStats);
						}
						JmxAccumulator<?> accumulator = accumulateJmxStatsAttrs(jmxStatsList);
						Map<String, TypeAndValue> nameToAccumulatedAttrForCurrentMapValue = new HashMap<>();
						nameToAccumulatedAttrForCurrentMapValue.put("_key", key.toString());
						nameToAccumulatedAttrForCurrentMapValue.putAll(fetchAttrNameToValueFromAccumulator(accumulator));
						CompositeData compositeData = buildCompositeDataFromMap(nameToAccumulatedAttrForCurrentMapValue);
						compositeDataList.add(compositeData);
					}

					check(compositeDataList.size() > 0);

					try {
						TabularType tabularType = new TabularType("TabularType", "TabularType",
								compositeDataList.get(0).getCompositeType(), new String[]{"_key"});
						TabularDataSupport tabularDataSupport = new TabularDataSupport(tabularType);
						for (CompositeData compositeData : compositeDataList) {
							tabularDataSupport.put(compositeData);
						}
						nameToAccumulatedAttr.put(attrName, tabularDataSupport);
					} catch (OpenDataException e) {
						throw new RuntimeException(e);
					}

				} else if (isPrimitiveType(mapValueClass) || isPrimitiveTypeWrapper(mapValueClass)
						|| isString(mapValueClass) || isThrowable(mapValueClass)) {
					List<CompositeData> compositeDataList = new ArrayList<>();
					for (Object key : groupedByKey.keySet()) {
						List<Object> values = new ArrayList<>();
						for (Object value : groupedByKey.get(key)) {
							values.add(value);
						}
						JmxAccumulator<?> accumulator = accumulateSimpleTypeAttrs(values);
						Map<String, Object> nameToAccumulatedAttrForCurrentMapValue = new HashMap<>();
						nameToAccumulatedAttrForCurrentMapValue.put("_key", key.toString());
						nameToAccumulatedAttrForCurrentMapValue.putAll(fetchAttrNameToValueFromAccumulatorIgnoreSkipName(accumulator));
						CompositeData compositeData = buildCompositeDataFromMap(nameToAccumulatedAttrForCurrentMapValue);
						compositeDataList.add(compositeData);
					}

					check(compositeDataList.size() > 0);

					try {
						TabularType tabularType = new TabularType("TabularType", "TabularType",
								compositeDataList.get(0).getCompositeType(), new String[]{"_key"});
						TabularDataSupport tabularDataSupport = new TabularDataSupport(tabularType);
						for (CompositeData compositeData : compositeDataList) {
							tabularDataSupport.put(compositeData);
						}
						nameToAccumulatedAttr.put(attrName, tabularDataSupport);
					} catch (OpenDataException e) {
						throw new RuntimeException(e);
					}
				} else {
					List<CompositeData> compositeDataList = new ArrayList<>();
					for (Object key : groupedByKey.keySet()) {
						List<Object> values = new ArrayList<>();
						for (Object value : groupedByKey.get(key)) {
							values.add(value);
						}
						JmxAccumulator<?> accumulator = accumulateSimpleTypeAttrs(values);
						Map<String, Object> nameToAccumulatedAttrForCurrentMapValue = new HashMap<>();
						nameToAccumulatedAttrForCurrentMapValue.put("_key", key.toString());
						nameToAccumulatedAttrForCurrentMapValue.putAll(fetchAttrNameToValueFromAccumulatorIgnoreSkipName(accumulator));
						CompositeData compositeData = buildCompositeDataFromMap(nameToAccumulatedAttrForCurrentMapValue);
						compositeDataList.add(compositeData);
					}

					check(compositeDataList.size() > 0);

					try {
						TabularType tabularType = new TabularType("TabularType", "TabularType",
								compositeDataList.get(0).getCompositeType(), new String[]{"_key"});
						TabularDataSupport tabularDataSupport = new TabularDataSupport(tabularType);
						for (CompositeData compositeData : compositeDataList) {
							tabularDataSupport.put(compositeData);
						}
						nameToAccumulatedAttr.put(attrName, tabularDataSupport);
					} catch (OpenDataException e) {
						throw new RuntimeException(e);
					}
				} // TODO (vmykhalko) add if-else case for array and list


//				for (Object object : objects) {
//					try {
//						List<?> currentList = (List<?>) getter.invoke(object);
//						accumulator.add(currentList);
//					} catch (IllegalAccessException | InvocationTargetException e) {
//						logger.error(
//								format("Cannot fetch list attribute with name \"%s\" during accumulation", attrName),
//								e);
//						throw new RuntimeException(e);
//					}
//
//				}
//				extractAttrsFromAccumulator(nameToAccumulatedAttr, attrName, accumulator);
			}
			return nameToAccumulatedAttr;
		}

		private static Object extractOneValueFromMap(Map<?, ?> map) {
			Iterator<?> iterator = map.values().iterator();
			return iterator.next();
		}

		private static Map<?, ?> findFirstNotEmptyMap(List<?> objects, Method mapGetter) {
			for (Object object : objects) {
				try {
					Map<?, ?> map = (Map<?, ?>) mapGetter.invoke(object);
					if (map != null && map.size() > 0) {
						return map;
					}
				} catch (IllegalAccessException | InvocationTargetException e) {
					throw new RuntimeException(e);
				}
			}
			return null;
		}

		private static Map<Object, List<Object>> fetchMapsAndGroupEntriesByKey(List<?> objects, Method mapGetter) {
			List<Map<?, ?>> listOfMaps = new ArrayList<>();
			for (Object object : objects) {
				try {
					Map<?, ?> map = (Map<?, ?>) mapGetter.invoke(object);
					if (map != null && map.size() > 0) {
						listOfMaps.add(map);
					}
				} catch (IllegalAccessException | InvocationTargetException e) {
					throw new RuntimeException(e);
				}
			}

			Map<Object, List<Object>> grouped = new HashMap<>();
			for (Map<?, ?> currentMap : listOfMaps) {
				for (Object key : currentMap.keySet()) {
					if (!grouped.containsKey(key)) {
						grouped.put(key, new ArrayList<>());
					}
					grouped.get(key).add(currentMap.get(key));
				}
			}
			return grouped;
		}

		private static Map<String, Object> fetchAndAccumulateThrowableAttrs(List<?> objects) {
			// TODO (vmykhalko): add preconditions

			Map<String, Object> nameToAccumulatedAttr = new HashMap<>();
			Object first = objects.get(0);
			List<Method> getters = fetchThrowableAttrGetters(first.getClass());
			for (Method getter : getters) {
				String attrName = extractFieldNameFromGetter(getter);
				JmxAccumulator<Object> accumulator = JmxAccumulators.getEquivalenceAccumulator();
				for (Object object : objects) {
					try {
						Throwable currentThrowable = (Throwable) getter.invoke(object);
						accumulator.add(currentThrowable);
					} catch (IllegalAccessException | InvocationTargetException e) {
						logger.error(
								format("Cannot fetch throwable attribute with name \"%s\" during accumulation",
										attrName),
								e);
						throw new RuntimeException(e);
					}
				}
				nameToAccumulatedAttr.putAll(extractAttrsFromAccumulator(attrName, accumulator));
			}
			return nameToAccumulatedAttr;
		}

		private static Map<String, Object> fetchAndAccumulateJmxStatsAttrs(List<?> objects) {
			// TODO (vmykhalko): add preconditions

			Map<String, Object> nameToAccumulatedAttr = new HashMap<>();
			Object first = objects.get(0);
			List<Method> getters = fetchJmxStatsAttrGetters(first.getClass());
			for (Method getter : getters) {
				String attrName = extractFieldNameFromGetter(getter);
				List<JmxStats<?>> jmxStatsList = new ArrayList<>();
				for (Object object : objects) {
					try {
						JmxStats<?> currentJmxStats = (JmxStats<?>) getter.invoke(object);
						jmxStatsList.add(currentJmxStats);
					} catch (IllegalAccessException | InvocationTargetException e) {
						logger.error(
								format("Cannot fetch JmxStats attribute with name \"%s\" during accumulation",
										attrName),
								e);
						throw new RuntimeException(e);
					}
				}
				nameToAccumulatedAttr.putAll(
						extractAttrsFromAccumulator(attrName, accumulateJmxStatsAttrs(jmxStatsList)));
			}
			return nameToAccumulatedAttr;
		}

		@SuppressWarnings("unchecked")
		private static JmxAccumulator<?> accumulateJmxStatsAttrs(List<JmxStats<?>> jmxStatsList) {
			checkArgument(jmxStatsList.size() > 0);

			JmxAccumulator accumulator;
			try {
				JmxStats<?> firstStats = jmxStatsList.get(0);
				accumulator = JmxAccumulators.getJmxStatsAccumulatorFor(firstStats.getClass());
			} catch (InstantiationException | IllegalAccessException e) {
				throw new RuntimeException(e);
			}
			for (JmxStats<?> jmxStats : jmxStatsList) {
				accumulator.add(jmxStats);
			}
			return accumulator;
		}

		private static JmxAccumulator<?> accumulateSimpleTypeAttrs(List<?> values) {
			JmxAccumulator<Object> accumulator = JmxAccumulators.getEquivalenceAccumulator();
			for (Object value : values) {
				accumulator.add(value);
			}
			return accumulator;
		}

		private static Map<String, TypeAndValue> extractAttrsFromAccumulator(
				String attrName, JmxAccumulator<?> accumulator) {

			Map<String, TypeAndValue> subAttrs = fetchAttrNameToValueFromAccumulator(accumulator);
			Map<String, TypeAndValue> attrs = new HashMap<>();
			if (subAttrs.size() == 1 && subAttrs.containsKey(NO_NAME_ATTR_KEY)) {
				attrs.put(attrName, subAttrs.get(NO_NAME_ATTR_KEY));
			} else {
				for (String subName : subAttrs.keySet()) {
					attrs.put(attrName + "_" + subName, subAttrs.get(subName));
				}
			}
			return attrs;
		}

		@Override
		public MBeanInfo getMBeanInfo() {
			return mBeanInfo;
		}

		private static final class AttributesSnapshot {
			private final long timestamp;
			private final Map<String, Object> nameToAttribute;

			public AttributesSnapshot(long timestamp, Map<String, Object> nameToAttribute) {
				this.timestamp = timestamp;
				this.nameToAttribute = nameToAttribute;
			}

			public long getTimestamp() {
				return timestamp;
			}

			public Object getAttribute(String attrName) {
				return nameToAttribute.get(attrName);
			}

			public boolean containsAttribute(String attrName) {
				return nameToAttribute.containsKey(attrName);
			}
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
			// actually
			this.jmxStatsGetters = jmxStatsGetters;
			this.jmxStatsList = jmxStatsList;
			this.simpleAttributeGetters = simpleAttributeGetters;
			this.simpleAttributeSetters = simpleAttributeSetters;
			this.listAttributeGetters = listAttributeGetters;
			this.arrayAttributeGetters = arrayAttributeGetters;
			this.throwableAttributeGetters = throwableAttributeGetters;
			this.operationKeyToMethod = operationKeyToMethod;
		}

		public JmxStats<?> getJmxStats(String jmxStatsName)
				throws IllegalArgumentException, InvocationTargetException, IllegalAccessException {
			Method jmxStatsGetter = jmxStatsGetters.get(jmxStatsName);
			if (jmxStatsGetter == null) {
				throw new IllegalArgumentException(
						format("Getter for JmxStats with name \"%s\" not found", jmxStatsName));
			}
			return (JmxStats<?>) jmxStatsGetter.invoke(monitorable);
		}

		public Executor getExecutor() {
			return monitorable.getJmxExecutor();
		}

		public List<? extends JmxStats<?>> getAllJmxStats() {
			// TODO (vmykhalko): according to RPC (stats per connection) use-case,
			// TODO we should use getters but not cached version of stats
			return jmxStatsList;
		}

		public ConcurrentJmxMBean getMonitorable() {
			return monitorable;
		}

		public Object getSimpleAttributeValue(String attrName)
				throws IllegalArgumentException, InvocationTargetException, IllegalAccessException {
			Method simpleAttributeGetter = simpleAttributeGetters.get(attrName);
			if (simpleAttributeGetter == null) {
				throw new IllegalArgumentException(
						format("Getter for simple-type attribute with name \"%s\" not found", attrName));
			}
			return simpleAttributeGetter.invoke(monitorable);
		}

		public List<?> getListAttributeValue(String attrName)
				throws IllegalArgumentException, InvocationTargetException, IllegalAccessException {
			Method listAttributeGetter = listAttributeGetters.get(attrName);
			if (listAttributeGetter == null) {
				throw new IllegalArgumentException(
						format("Getter for List attribute with name \"%s\" not found", attrName));
			}
			return (List<?>) listAttributeGetter.invoke(monitorable);
		}

		public Object[] getArrayAttributeValue(String attrName)
				throws IllegalArgumentException, InvocationTargetException, IllegalAccessException {
			Method arrayAttributeGetter = arrayAttributeGetters.get(attrName);
			if (arrayAttributeGetter == null) {
				throw new IllegalArgumentException(
						format("Getter for Array attribute with name \"%s\" not found", attrName));
			}
			return (Object[]) arrayAttributeGetter.invoke(monitorable);
		}

		public Throwable getThrowableAttributeValue(String attrName)
				throws IllegalArgumentException, InvocationTargetException, IllegalAccessException {
			Method throwableAttributeGetter = throwableAttributeGetters.get(attrName);
			if (throwableAttributeGetter == null) {
				throw new IllegalArgumentException(
						format("Getter for Throwable attribute with name \"%s\" not found", attrName));
			}
			return (Throwable) throwableAttributeGetter.invoke(monitorable);
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
