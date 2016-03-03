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
import javax.management.openmbean.OpenType;
import java.lang.annotation.Annotation;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.util.*;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static io.datakernel.jmx.Utils.*;
import static io.datakernel.util.Preconditions.*;
import static java.lang.String.format;
import static java.util.Arrays.asList;

public final class JmxMBeans implements DynamicMBeanFactory {
	private static final Logger logger = LoggerFactory.getLogger(JmxMBeans.class);

	private static final Timer TIMER = new Timer(true);
	private static final String REFRESH_PERIOD_ATTRIBUTE_NAME = "_refreshPeriod";
	private static final String SMOOTHING_WINDOW_ATTRIBUTE_NAME = "_smoothingWindow";
	private static final String SET_REFRESH_PERIOD_OP_NAME = "_setRefreshPeriod";
	private static final String SET_SMOOTHING_WINDOW_OP_NAME = "_setSmoothingWindow";
	private static final String SET_REFRESH_PERIOD_PARAMETER_NAME = "period";
	private static final String SET_SMOOTHING_WINDOW_PARAMETER_NAME = "window";
	public static final double DEFAULT_REFRESH_PERIOD = 0.2;
	public static final double DEFAULT_SMOOTHING_WINDOW = 10.0;

	private static final CurrentTimeProvider TIME_PROVIDER = CurrentTimeProviderSystem.instance();

	private JmxMBeans() {}

	public static JmxMBeans factory() {
		return new JmxMBeans();
	}

	@Override
	public DynamicMBean createFor(List<? extends ConcurrentJmxMBean> monitorables, boolean enableRefresh) throws ReflectiveOperationException {
		checkNotNull(monitorables);
		checkArgument(monitorables.size() > 0);
		checkArgument(!listContainsNullValues(monitorables), "monitorable can not be null");
		checkArgument(allObjectsAreOfSameType(monitorables));

		// all objects are of same type, so we can extract info from any of them
		Class<? extends ConcurrentJmxMBean> mbeanClass = monitorables.get(0).getClass();
		AttributeNodeForPojo rootNode = createAttributesTree(mbeanClass);
		MBeanInfo mBeanInfo = createMBeanInfo(rootNode, mbeanClass, enableRefresh);
		Map<OperationKey, Method> opkeyToMethod = fetchOpkeyToMethod(mbeanClass);

		DynamicMBeanAggregator mbean = new DynamicMBeanAggregator(
				mBeanInfo, monitorables, rootNode, opkeyToMethod, enableRefresh
		);

		if (enableRefresh) {
			startRefreshing(mbeanClass, mbean);
		}

		return mbean;
	}

	private static void startRefreshing(Class<? extends ConcurrentJmxMBean> mbeanClass, DynamicMBeanAggregator mbean) {
		double smoothingWindow = DEFAULT_SMOOTHING_WINDOW;
		double refreshPeriod = DEFAULT_REFRESH_PERIOD;
		JmxRefreshSettings settings = fetchRefreshSettingsIfExists(mbeanClass);
		if (settings != null) {
			double customSmoothingWindow = settings.smoothingWindow();
			double customRefreshPeriod = settings.period();
			checkArgument(customRefreshPeriod > 0.0, "refresh period must be positive");
			checkArgument(customSmoothingWindow > 0.0, "smoothing window must be positive");
			smoothingWindow = customSmoothingWindow;
			refreshPeriod = customRefreshPeriod;
		}
		mbean.startRefreshing(refreshPeriod, smoothingWindow);
	}

	private static JmxRefreshSettings fetchRefreshSettingsIfExists(Class<?> clazz) {
		Class<?> currentClass = clazz;
		while (currentClass != null) {
			if (currentClass.isAnnotationPresent(JmxRefreshSettings.class)) {
				return currentClass.getAnnotation(JmxRefreshSettings.class);
			}
			currentClass = currentClass.getSuperclass();
		}
		return null;
	}

	private static AttributeNodeForPojo createAttributesTree(Class<? extends ConcurrentJmxMBean> clazz) {
		List<AttributeNode> subNodes = createNodesFor(clazz);
		AttributeNodeForPojo root = new AttributeNodeForPojo("", new ValueFetcherDirect(), subNodes);
		return root;
	}

	private static List<AttributeNode> createNodesFor(Class<?> clazz) {
		List<AttributeDescriptor> attrDescriptors = fetchAttributeDescriptors(clazz);
		List<AttributeNode> attrNodes = new ArrayList<>();
		for (AttributeDescriptor descriptor : attrDescriptors) {
			check(descriptor.getGetter() != null, "@JmxAttribute \"%s\" does not have getter", descriptor.getName());

			String attrName;
			Method getter = descriptor.getGetter();
			JmxAttribute attrAnnotation = getter.getAnnotation(JmxAttribute.class);
			String attrAnnotationName = attrAnnotation.name();
			if (attrAnnotationName.equals(JmxAttribute.USE_GETTER_NAME)) {
				attrName = extractFieldNameFromGetter(getter);
			} else {
				attrName = attrAnnotationName;
			}
			checkArgument(!attrName.contains("_"), "@JmxAttribute with name \"%s\" contains underscores", attrName);
			Type type = getter.getGenericReturnType();
			attrNodes.add(createAttributeNodeFor(attrName, type, getter, descriptor.getSetter()));
		}
		return attrNodes;
	}

//	private static List<Method> extractAttributeGettersFrom(Class<?> clazz) {
//		Method[] methods = clazz.getMethods();
//		List<Method> attrGetters = new ArrayList<>();
//		for (Method method : methods) {
//			if (method.isAnnotationPresent(JmxAttribute.class)) {
//				if (!isGetter(method)) {
//					logger.warn(
//							format("Method \"%s\" in class \"%s\" is annotated with @JmxAttribute but is not getter",
//									method.getName(), clazz.getName()));
//					continue;
//				}
//				attrGetters.add(method);
//			}
//		}
//		return attrGetters;
//	}

	private static List<AttributeDescriptor> fetchAttributeDescriptors(Class<?> clazz) {
		Map<String, AttributeDescriptor> nameToAttr = new HashMap<>();
		for (Method method : clazz.getMethods()) {
			if (method.isAnnotationPresent(JmxAttribute.class)) {
				if (isGetter(method)) {
					processGetter(nameToAttr, method);
				} else if (isSetter(method)) {
					processSetter(nameToAttr, method);
				} else {
					throw new RuntimeException(format("Method \"%s\" of class \"%s\" is annotated with @JmxAnnotation "
							+ "but is neither getter nor setter", method.getName(), method.getClass().getName())
					);
				}
			}
		}
		return new ArrayList<>(nameToAttr.values());
	}

	private static void processGetter(Map<String, AttributeDescriptor> nameToAttr, Method getter) {
		String name = extractFieldNameFromGetter(getter);
		Type attrType = getter.getReturnType();
		if (nameToAttr.containsKey(name)) {
			AttributeDescriptor previousDescriptor = nameToAttr.get(name);

			check(previousDescriptor.getGetter() == null,
					"More that one getter with name" + getter.getName());
			check(previousDescriptor.getType().equals(attrType),
					"Getter with name \"%s\" has different type than appropriate setter", getter.getName());

			nameToAttr.put(name, new AttributeDescriptor(
					name, attrType, getter, previousDescriptor.getSetter()));
		} else {
			nameToAttr.put(name, new AttributeDescriptor(name, attrType, getter, null));
		}
	}

	private static void processSetter(Map<String, AttributeDescriptor> nameToAttr, Method setter) {
		checkArgument(isSimpleType(setter.getParameterTypes()[0]), "Setters are allowed only on SimpleType attributes."
				+ " But setter \"%s\" is not SimpleType setter", setter.getName());

		String name = extractFieldNameFromSetter(setter);
		Type attrType = setter.getParameterTypes()[0];
		if (nameToAttr.containsKey(name)) {
			AttributeDescriptor previousDescriptor = nameToAttr.get(name);

			check(previousDescriptor.getSetter() == null,
					"More that one setter with name" + setter.getName());
			check(previousDescriptor.getType().equals(attrType),
					"Setter with name \"%s\" has different type than appropriate getter", setter.getName());

			nameToAttr.put(name, new AttributeDescriptor(
					name, attrType, previousDescriptor.getGetter(), setter));
		} else {
			nameToAttr.put(name, new AttributeDescriptor(name, attrType, null, setter));
		}
	}

	private static AttributeNode createAttributeNodeFor(String attrName, Type attrType, Method getter, Method setter) {
		ValueFetcher defaultFetcher = getter != null ? new ValueFetcherFromGetter(getter) : new ValueFetcherDirect();
		if (attrType instanceof Class) {
			// 3 cases: simple-type, JmxStats, POJO
			Class<?> returnClass = (Class<?>) attrType;
			if (isSimpleType(returnClass)) {
				return new AttributeNodeForSimpleType(attrName, defaultFetcher, setter, returnClass);
			} else if (isThrowable(returnClass)) {
				return new AttributeNodeForThrowable(attrName, defaultFetcher);
			} else if (returnClass.isArray()) {
				Class<?> elementType = returnClass.getComponentType();
				checkNotNull(getter, "Arrays can be used only directly in POJO, JmxStats or JmxMBeans");
				ValueFetcher fetcher = new ValueFetcherFromGetterArrayAdapter(getter);
				return createListAttributeNodeFor(attrName, fetcher, elementType);
			} else if (isJmxStats(returnClass)) {
				// JmxStats case
				List<AttributeNode> subNodes = createNodesFor(returnClass);

				if (subNodes.size() == 0) {
					throw new IllegalArgumentException(format(
							"JmxStats of type \"%s\" does not have JmxAttributes",
							returnClass.getName()));
				}

				return new AttributeNodeForJmxStats(attrName, defaultFetcher, returnClass, subNodes);
			} else {
				// POJO case
				List<AttributeNode> subNodes = createNodesFor(returnClass);

				if (subNodes.size() == 0) {
					throw new IllegalArgumentException(format(
							"Type \"%s\" seems to be POJO but does not have JmxAttributes",
							returnClass.getName()));
				}

				return new AttributeNodeForPojo(attrName, defaultFetcher, subNodes);
			}
		} else if (attrType instanceof ParameterizedType) {
			return createNodeForParametrizedType(attrName, (ParameterizedType) attrType, getter);
		} else {
			throw new RuntimeException();
		}
	}

	private static AttributeNode createNodeForParametrizedType(String attrName, ParameterizedType pType,
	                                                           Method getter) {
		ValueFetcher fetcher = createAppropriateFetcher(getter);
		Class<?> rawType = (Class<?>) pType.getRawType();
		if (rawType == List.class) {
			Type listElementType = pType.getActualTypeArguments()[0];
			return createListAttributeNodeFor(attrName, fetcher, listElementType);
		} else if (rawType == Map.class) {
			Type valueType = pType.getActualTypeArguments()[1];
			return createMapAttributeNodeFor(attrName, fetcher, valueType);
		} else {
			throw new RuntimeException("There is no support for Generic classes other than List or Map");
		}
	}

	private static AttributeNodeForList createListAttributeNodeFor(String attrName, ValueFetcher fetcher,
	                                                               Type listElementType) {
		if (listElementType instanceof Class<?>) {
			String typeName = ((Class<?>) listElementType).getSimpleName();
			return new AttributeNodeForList(attrName, fetcher,
					createAttributeNodeFor(typeName, listElementType, null, null));
		} else if (listElementType instanceof ParameterizedType) {
			String typeName = ((Class<?>) ((ParameterizedType) listElementType).getRawType()).getSimpleName();
			return new AttributeNodeForList(attrName, fetcher,
					createNodeForParametrizedType(typeName, (ParameterizedType) listElementType, null));
		} else {
			throw new RuntimeException();
		}
	}

	private static AttributeNodeForMap createMapAttributeNodeFor(String attrName, ValueFetcher fetcher,
	                                                             Type valueType) {
		if (valueType instanceof Class<?>) {
			String typeName = ((Class<?>) valueType).getSimpleName();
			return new AttributeNodeForMap(attrName, fetcher,
					createAttributeNodeFor(typeName, valueType, null, null));
		} else if (valueType instanceof ParameterizedType) {
			String typeName = ((Class<?>) ((ParameterizedType) valueType).getRawType()).getSimpleName();
			return new AttributeNodeForMap(attrName, fetcher,
					createNodeForParametrizedType(typeName, (ParameterizedType) valueType, null));
		} else {
			throw new RuntimeException();
		}
	}

	private static boolean isSimpleType(Class<?> clazz) {
		return isPrimitiveType(clazz) || isPrimitiveTypeWrapper(clazz) || isString(clazz);
	}

	private static ValueFetcher createAppropriateFetcher(Method getter) {
		return getter != null ? new ValueFetcherFromGetter(getter) : new ValueFetcherDirect();
	}

	private static MBeanOperationInfo[] fetchOperationsInfo(Class<?> monitorableClass, boolean enableRefresh)
			throws InvocationTargetException, IllegalAccessException {
		// TODO(vmykhalko): refactor this method
		List<MBeanOperationInfo> operations = new ArrayList<>();
		Method[] methods = monitorableClass.getMethods();
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

	private static MBeanInfo createMBeanInfo(AttributeNodeForPojo rootNode,
	                                         Class<? extends ConcurrentJmxMBean> monitorableClass,
	                                         boolean enableRefresh)
			throws InvocationTargetException, IllegalAccessException {
		String monitorableName = "";
		String monitorableDescription = "";
		MBeanAttributeInfo[] attributes = fetchAttributesInfo(rootNode, enableRefresh);
		MBeanOperationInfo[] operations = fetchOperationsInfo(monitorableClass, enableRefresh);
		return new MBeanInfo(
				monitorableName,
				monitorableDescription,
				attributes,
				null,  // constructors
				operations,
				null); //notifications
	}

	private static MBeanAttributeInfo[] fetchAttributesInfo(AttributeNodeForPojo rootNode, boolean refreshEnabled) {
		Map<String, OpenType<?>> nameToType = rootNode.getFlattenedOpenTypes();
		List<MBeanAttributeInfo> attrsInfo = new ArrayList<>();
		for (String attrName : nameToType.keySet()) {
			String attrType = nameToType.get(attrName).getClassName();
			boolean writable = rootNode.isSettable(attrName);
			attrsInfo.add(new MBeanAttributeInfo(attrName, attrType, attrName, true, writable, false));
		}

		if (refreshEnabled) {
			attrsInfo.addAll(createAttributesForRefreshControl());
		}

		return attrsInfo.toArray(new MBeanAttributeInfo[attrsInfo.size()]);
	}

	private static List<MBeanAttributeInfo> createAttributesForRefreshControl() {
		List<MBeanAttributeInfo> refreshAttrs = new ArrayList<>();
		MBeanAttributeInfo refreshPeriodAttr =
				new MBeanAttributeInfo(REFRESH_PERIOD_ATTRIBUTE_NAME, "double", REFRESH_PERIOD_ATTRIBUTE_NAME,
						true, false, false);
		refreshAttrs.add(refreshPeriodAttr);
		MBeanAttributeInfo smoothingWindowAttr =
				new MBeanAttributeInfo(SMOOTHING_WINDOW_ATTRIBUTE_NAME, "double", SMOOTHING_WINDOW_ATTRIBUTE_NAME,
						true, false, false);
		refreshAttrs.add(smoothingWindowAttr);
		return refreshAttrs;
	}

	// TODO(vmykhalko): refactor this method (it has common code with  fetchOperationsInfo()
	private static Map<OperationKey, Method> fetchOpkeyToMethod(Class<? extends ConcurrentJmxMBean> mbeanClass) {
		Map<OperationKey, Method> opkeyToMethod = new HashMap<>();
		Method[] methods = mbeanClass.getMethods();
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

	private static final class AttributeDescriptor {
		private final String name;
		private final Type type;
		private final Method getter;
		private final Method setter;

		public AttributeDescriptor(String name, Type type, Method getter, Method setter) {
			this.name = name;
			this.type = type;
			this.getter = getter;
			this.setter = setter;
		}

		public String getName() {
			return name;
		}

		public Type getType() {
			return type;
		}

		public Method getGetter() {
			return getter;
		}

		public Method getSetter() {
			return setter;
		}
	}

//	private static String extractNameFromAttributeGetter(Method getter);
//	private static String extractNameFromAttributeSetter(Method setter);

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
		private final List<? extends ConcurrentJmxMBean> mbeans;
		private final AttributeNodeForPojo rootNode;
		private final Map<OperationKey, Method> opkeyToMethod;

		// refresh
		private boolean refreshEnabled;
		private volatile double refreshPeriod;
		private volatile double smoothingWindow;

		public DynamicMBeanAggregator(MBeanInfo mBeanInfo, List<? extends ConcurrentJmxMBean> mbeans,
		                              AttributeNodeForPojo rootNode, Map<OperationKey, Method> opkeyToMethod,
		                              boolean refreshEnabled) {
			this.mBeanInfo = mBeanInfo;
			this.mbeans = mbeans;
			this.rootNode = rootNode;
			this.opkeyToMethod = opkeyToMethod;
			this.refreshEnabled = refreshEnabled;
		}

		public void startRefreshing(double defaultRefreshPeriod, double defaultSmoothingWindow) {
			if (rootNode.isRefreshable()) {
				checkState(refreshEnabled());
				refreshPeriod = defaultRefreshPeriod;
				smoothingWindow = defaultSmoothingWindow;
				TIMER.schedule(createRefreshTask(), 0L);
			} else {
				logger.warn("Refresh was enabled but MBeans are not refreshable");
			}
		}

		private boolean refreshEnabled() {
			return refreshEnabled;
		}

		private TimerTask createRefreshTask() {
			checkState(refreshEnabled());
			return new TimerTask() {
				@Override
				public void run() {
					final AtomicInteger mbeansLeftForRefresh = new AtomicInteger(mbeans.size());
					// cache smoothingWindow and refreshPeriod to be same for all localRefreshTasks
					// because this two parameters may be changed from other thread
					final double currentSmoothingWindow = smoothingWindow;
					final int currentRefreshPeriod = secondsToMillis(refreshPeriod);
					final long currentTimestamp = TIME_PROVIDER.currentTimeMillis();
					for (final ConcurrentJmxMBean mbean : mbeans) {
						final Executor executor = mbean.getJmxExecutor();
						checkNotNull(executor, "Error. Executor of ConcurrentMBean cannot be null");
//						final List<? extends JmxStats<?>> statsList = wrapper.getAllJmxStats();
						executor.execute(new Runnable() {
							@Override
							public void run() {
								rootNode.refresh(asList(mbean), currentTimestamp, currentSmoothingWindow);
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

			Object attrValue = rootNode.aggregateAttribute(attribute, mbeans);

			// TODO(vmykhalko): is support of AggregationException needed ?
//			if (attrValue instanceof Exception) {
//				Exception attrException = (Exception) attrValue;
//				throw new MBeanException(EXCEPTION,
//						format("Exception with type \"%s\" and message \"%s\" occured during fetching attribute",
//								attrException.getClass().getName(), attrException.getMessage()));
//			}

			return attrValue;
		}

		@Override
		public void setAttribute(final Attribute attribute) throws AttributeNotFoundException, InvalidAttributeValueException,
				MBeanException, ReflectionException {
			rootNode.setAttribute(attribute.getName(), attribute.getValue(), mbeans);
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

			String[] argTypes = signature != null ? signature : new String[0];
			final Object[] args = params != null ? params : new Object[0];
			OperationKey opkey = new OperationKey(actionName, argTypes);
			final Method opMethod = opkeyToMethod.get(opkey);
			if (opMethod == null) {
				String operationName = prettyOperationName(actionName, argTypes);
				String errorMsg = "There is no operation \"" + operationName + "\"";
				throw new RuntimeOperationsException(new IllegalArgumentException("Operation not found"), errorMsg);
			}

			final CountDownLatch latch = new CountDownLatch(mbeans.size());
			final AtomicReference<Exception> exceptionReference = new AtomicReference<>();

			for (final ConcurrentJmxMBean mbean : mbeans) {
				Executor executor = mbean.getJmxExecutor();
				executor.execute((new Runnable() {
					@Override
					public void run() {
						try {
							opMethod.invoke(mbean, args);
							latch.countDown();
						} catch (Exception e) {
							exceptionReference.set(e);
							latch.countDown();
						}
					}
				}));
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

		/**
		 * Returns true and perform action if action is for refresh control, otherwise just returns false
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
}
