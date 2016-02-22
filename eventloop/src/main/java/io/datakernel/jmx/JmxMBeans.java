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

	public static final long SNAPSHOT_UPDATE_DEFAULT_PERIOD = 200L; // milliseconds
	private static final CurrentTimeProvider TIME_PROVIDER = CurrentTimeProviderSystem.instance();
	private final long snapshotUpdatePeriod;

	//
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
		ConcurrentJmxMBean first = monitorables.get(0);
		PojoAttributeNode rootNode = createAttributesTree(first.getClass());
		MBeanInfo mBeanInfo = createMBeanInfo(rootNode, first.getClass(), enableRefresh);
		Map<OperationKey, Method> opkeyToMethod = fetchOpkeyToMethod(first.getClass());

		DynamicMBeanAggregator mbean = new DynamicMBeanAggregator(
				mBeanInfo, monitorables, rootNode, opkeyToMethod, snapshotUpdatePeriod, enableRefresh
		);

		if (enableRefresh) {
			mbean.startRefreshing(DEFAULT_REFRESH_PERIOD, DEFAULT_SMOOTHING_WINDOW);
		}

		return mbean;
	}

	public static PojoAttributeNode createAttributesTree(Class<?> clazz) {
		List<AttributeNode> subNodes = createNodesFor(clazz);
		PojoAttributeNode root = new PojoAttributeNode("", new DirectFetcher(), subNodes);
		return root;
	}

	private static List<AttributeNode> createNodesFor(Class<?> clazz) {
		List<Method> getters = extractAttributeGettersFrom(clazz);
		List<AttributeNode> attrNodes = new ArrayList<>();
		for (Method getter : getters) {
			String attrName = extractFieldNameFromGetter(getter);
			Type returnType = getter.getGenericReturnType();
			attrNodes.add(createAttributeNodeFor(attrName, returnType, getter));
		}
		return attrNodes;
	}

	private static List<Method> extractAttributeGettersFrom(Class<?> clazz) {
		Method[] methods = clazz.getMethods();
		List<Method> attrGetters = new ArrayList<>();
		for (Method method : methods) {
			if (method.isAnnotationPresent(JmxAttribute.class)) {
				if (!isGetter(method)) {
					logger.warn(
							format("Method \"%s\" in class \"%s\" is annotated with @JmxAttribute but is not getter",
									method.getName(), clazz.getName()));
					continue;
				}
				attrGetters.add(method);
			}
		}
		return attrGetters;
	}

	private static AttributeNode createAttributeNodeFor(String attrName, Type attrType, Method getter) {
		ValueFetcher fetcher = getter != null ? new FetcherFromGetter(getter) : new DirectFetcher();
		if (attrType instanceof Class) {
			// 3 cases: simple-type, JmxStats, POJO
			Class<?> returnClass = (Class<?>) attrType;
			if (isSimpleType(returnClass)) {
				return new SimpleTypeAttributeNode(attrName, fetcher, returnClass);
			} else if (isJmxStats(returnClass)) {
				// JmxStats case
				List<AttributeNode> subNodes = createNodesFor(returnClass);

				if (subNodes.size() == 0) {
					logger.warn(format(
							"JmxStats of type \"%s\" does not have JmxAttributes",
							returnClass.getName()));
					return null;
				}

				return new JmxStatsAttributeNode(attrName, fetcher, returnClass, subNodes);
			} else {
				// POJO case
				List<AttributeNode> subNodes = createNodesFor(returnClass);

				if (subNodes.size() == 0) {
					logger.warn(format(
							"Type \"%s\" seems to be POJO but does not have JmxAttributes", returnClass.getName()));
					return null;
				}

				return new PojoAttributeNode(attrName, fetcher, subNodes);
			}
		} else if (attrType instanceof ParameterizedType) {
			return createNodeForParametrizedType(attrName, (ParameterizedType) attrType, getter);
		} else {
			throw new RuntimeException();
		}
	}

	private static AttributeNode createNodeForParametrizedType(String attrName, ParameterizedType pType,
	                                                           Method getter) {
		Class<?> rawType = (Class<?>) pType.getRawType();
		if (rawType == List.class) {
			Type listElementType = pType.getActualTypeArguments()[0];
			return createListAttributeNodeFor(attrName, getter, listElementType);
		} else if (rawType == Map.class) {
			Type valueType = pType.getActualTypeArguments()[1];
			return createMapAttributeNodeFor(attrName, getter, valueType);
		} else {
			throw new RuntimeException("There is no support for Generic classes other than List or Map");
		}
	}

	private static ListAttributeNode createListAttributeNodeFor(String attrName, Method getter, Type listElementType) {
		ValueFetcher fetcher = createAppropriateFetcher(getter);
		if (listElementType instanceof Class<?>) {
			return new ListAttributeNode(attrName, fetcher, createAttributeNodeFor("", listElementType, null));
		} else if (listElementType instanceof ParameterizedType) {
			return new ListAttributeNode(attrName, fetcher,
					createNodeForParametrizedType("", (ParameterizedType) listElementType, null));
		} else {
			throw new RuntimeException();
		}
	}

	private static MapAttributeNode createMapAttributeNodeFor(String attrName, Method getter, Type valueType) {
		ValueFetcher fetcher = createAppropriateFetcher(getter);
		if (valueType instanceof Class<?>) {
			return new MapAttributeNode(attrName, fetcher, createAttributeNodeFor("", valueType, null));
		} else if (valueType instanceof ParameterizedType) {
			return new MapAttributeNode(attrName, fetcher,
					createNodeForParametrizedType("", (ParameterizedType) valueType, null));
		} else {
			throw new RuntimeException();
		}
	}

	private static boolean isSimpleType(Class<?> clazz) {
		return isPrimitiveType(clazz) || isPrimitiveTypeWrapper(clazz) || isString(clazz);
	}

	private static ValueFetcher createAppropriateFetcher(Method getter) {
		return getter != null ? new FetcherFromGetter(getter) : new DirectFetcher();
	}

	private static MBeanOperationInfo[] extractOperationsInfo(Class<?> monitorableClass, boolean enableRefresh)
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

	private static MBeanInfo createMBeanInfo(PojoAttributeNode rootNode, Class<?> monitorableClass,
	                                         boolean enableRefresh)
			throws InvocationTargetException, IllegalAccessException {
		String monitorableName = "";
		String monitorableDescription = "";
		MBeanAttributeInfo[] attributes = extractAttributesInfo(rootNode);
		MBeanOperationInfo[] operations = extractOperationsInfo(monitorableClass, enableRefresh);
		return new MBeanInfo(
				monitorableName,
				monitorableDescription,
				attributes,
				null,  // constructors
				operations,
				null); //notifications
	}

	private static MBeanAttributeInfo[] extractAttributesInfo(PojoAttributeNode rootNode) {
		Map<String, OpenType<?>> nameToType = rootNode.getFlattenedOpenTypes();
		List<MBeanAttributeInfo> attrsInfo = new ArrayList<>();
		for (String attrName : nameToType.keySet()) {
			String attrType = nameToType.get(attrName).getClassName();
			attrsInfo.add(new MBeanAttributeInfo(attrName, attrType, attrName, true, false, false));
		}
		return attrsInfo.toArray(new MBeanAttributeInfo[attrsInfo.size()]);
	}

	// TODO(vmykhalko): refactor this method (it has common code with  extractOperationsInfo()
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
		private final PojoAttributeNode rootNode;
		private final Map<OperationKey, Method> opkeyToMethod;

		// refresh
		private boolean refreshEnabled;
		private final long snapshotUpdatePeriod;
		private volatile double refreshPeriod;
		private volatile double smoothingWindow;

//		private final Map<String, Class<? extends JmxStats<?>>> nameToJmxStatsType;
//		private final Map<String, MBeanAttributeInfo> nameToSimpleAttribute;
//		private final Set<String> listAttributes;
//		private final Set<String> arrayAttributes;
//		private final Set<String> exceptionAttributes;
//
//		private volatile AttributesSnapshot lastAttributesSnapshot;
//
//		private final long snapshotUpdatePeriod;
//
//		// refresh
//		private volatile double refreshPeriod;
//		private volatile double smoothingWindow;
//		private boolean refreshEnabled;

		public DynamicMBeanAggregator(MBeanInfo mBeanInfo, List<? extends ConcurrentJmxMBean> mbeans,
		                              PojoAttributeNode rootNode, Map<OperationKey, Method> opkeyToMethod,
		                              long snapshotUpdatePeriod, boolean refreshEnabled) {
			this.mBeanInfo = mBeanInfo;
			this.mbeans = mbeans;
			this.rootNode = rootNode;
			this.opkeyToMethod = opkeyToMethod;
			this.snapshotUpdatePeriod = snapshotUpdatePeriod;
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

			Object attrValue = rootNode.aggregateAttribute(mbeans, attribute);

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
//			MBeanAttributeInfo attrInfo = nameToSimpleAttribute.get(attribute.getName());
//			if (attrInfo == null) {
//				throw new AttributeNotFoundException(format("There is no attribute with name \"%s\"", attribute));
//			}
//			if (!attrInfo.isWritable()) {
//				throw new AttributeNotFoundException(format("Attribute with name \"%s\" is not writable", attribute));
//			}
//
//			final CountDownLatch latch = new CountDownLatch(wrappers.size());
//			final AtomicReference<Exception> exceptionReference = new AtomicReference<>();
//			for (final JmxMonitorableWrapper wrapper : wrappers) {
//				Executor executor = wrapper.getExecutor();
//				executor.execute(new Runnable() {
//					@Override
//					public void run() {
//						try {
//							wrapper.setSimpleAttribute(attribute.getName(), attribute.getValue());
//							latch.countDown();
//						} catch (Exception e) {
//							exceptionReference.set(e);
//							latch.countDown();
//						}
//					}
//				});
//			}
//
//			try {
//				latch.await();
//			} catch (InterruptedException e) {
//				throw new MBeanException(e);
//			}
//
//			Exception exception = exceptionReference.get();
//			if (exception != null) {
//				throw new MBeanException(exception);
//			}
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
