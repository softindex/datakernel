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

import io.datakernel.annotation.Nullable;
import io.datakernel.eventloop.Eventloop;
import io.datakernel.util.ReflectionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.management.*;
import javax.management.openmbean.OpenType;
import javax.management.openmbean.SimpleType;
import java.lang.annotation.Annotation;
import java.lang.reflect.*;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;

import static io.datakernel.util.CollectionUtils.first;
import static io.datakernel.util.Preconditions.*;
import static io.datakernel.util.ReflectionUtils.*;
import static java.lang.Math.ceil;
import static java.lang.String.format;
import static java.util.Arrays.asList;
import static java.util.Collections.singleton;
import static java.util.Collections.singletonList;

public final class JmxMBeans implements DynamicMBeanFactory {
	private static final Logger logger = LoggerFactory.getLogger(JmxMBeans.class);

	// refreshing jmx
	public static final Duration DEFAULT_REFRESH_PERIOD_IN_SECONDS = Duration.ofSeconds(1);
	public static final int MAX_JMX_REFRESHES_PER_ONE_CYCLE_DEFAULT = 500;
	private int maxJmxRefreshesPerOneCycle;
	private long specifiedRefreshPeriod;
	private final Map<Eventloop, List<JmxRefreshable>> eventloopToJmxRefreshables = new ConcurrentHashMap<>();
	private final Map<Eventloop, Integer> refreshableStatsCounts = new ConcurrentHashMap<>();
	private final Map<Eventloop, Integer> effectiveRefreshPeriods = new ConcurrentHashMap<>();
	private final Map<Type, Transformer<?>> customTypes = new HashMap<>();

	private static final JmxReducer<?> DEFAULT_REDUCER = new JmxReducers.JmxReducerDistinct();

	// JmxStats creator methods
	private static final String CREATE = "create";
	private static final String CREATE_ACCUMULATOR = "createAccumulator";

	private static final JmxMBeans INSTANCE_WITH_DEFAULT_REFRESH_PERIOD
			= new JmxMBeans(DEFAULT_REFRESH_PERIOD_IN_SECONDS, MAX_JMX_REFRESHES_PER_ONE_CYCLE_DEFAULT);

	// region constructor and factory methods
	private JmxMBeans(Duration refreshPeriod, int maxJmxRefreshesPerOneCycle) {
		this.specifiedRefreshPeriod = refreshPeriod.toMillis();
		this.maxJmxRefreshesPerOneCycle = maxJmxRefreshesPerOneCycle;
	}

	public static JmxMBeans factory() {
		return INSTANCE_WITH_DEFAULT_REFRESH_PERIOD;
	}

	public static JmxMBeans factory(Duration refreshPeriod, int maxJmxRefreshesPerOneCycle) {
		return new JmxMBeans(refreshPeriod, maxJmxRefreshesPerOneCycle);
	}
	// endregion

	// region exportable stats for JmxRegistry
	public Map<Eventloop, Integer> getRefreshableStatsCounts() {
		return refreshableStatsCounts;
	}

	public Map<Eventloop, Integer> getEffectiveRefreshPeriods() {
		return effectiveRefreshPeriods;
	}

	public Duration getSpecifiedRefreshPeriod() {
		return Duration.ofMillis(specifiedRefreshPeriod);
	}

	public void setRefreshPeriod(Duration refreshPeriod) {
		this.specifiedRefreshPeriod = refreshPeriod.toMillis();
	}

	public int getMaxJmxRefreshesPerOneCycle() {
		return maxJmxRefreshesPerOneCycle;
	}

	public void setMaxJmxRefreshesPerOneCycle(int maxJmxRefreshesPerOneCycle) {
		this.maxJmxRefreshesPerOneCycle = maxJmxRefreshesPerOneCycle;
	}
	// endregion

	@Override
	public DynamicMBean createFor(List<?> monitorables, MBeanSettings setting, boolean enableRefresh, Map<Type, Transformer<?>> customTypes) {
		checkNotNull(monitorables);
		checkArgument(monitorables.size() > 0);
		checkArgument(!listContainsNullValues(monitorables), "monitorable can not be null");
		checkArgument(allObjectsAreOfSameType(monitorables));

		this.customTypes.putAll(customTypes);

		Object firstMBean = monitorables.get(0);
		Class<?> mbeanClass = firstMBean.getClass();

		boolean isRefreshEnabled = enableRefresh;

		List<MBeanWrapper> mbeanWrappers = new ArrayList<>(monitorables.size());
		if (ConcurrentJmxMBean.class.isAssignableFrom(mbeanClass)) {
			checkArgument(monitorables.size() == 1, "ConcurrentJmxMBeans cannot be used in pool. " +
					"Only EventloopJmxMBeans can be used in pool");
			isRefreshEnabled = false;
			mbeanWrappers.add(new ConcurrentJmxMBeanWrapper((ConcurrentJmxMBean) monitorables.get(0)));
		} else if (EventloopJmxMBean.class.isAssignableFrom(mbeanClass)) {
			for (Object monitorable : monitorables) {
				mbeanWrappers.add(new EventloopJmxMBeanWrapper((EventloopJmxMBean) monitorable));
			}
		} else {
			throw new IllegalArgumentException("MBeans should implement either ConcurrentJmxMBean " +
					"or EventloopJmxMBean interface");
		}

		AttributeNodeForPojo rootNode = createAttributesTree(mbeanClass);
		rootNode.hideNullPojos(monitorables);

		for (String included : setting.getIncludedOptionals()) {
			rootNode.setVisible(included);
		}

		// TODO(vmykhalko): check in JmxRegistry that modifiers are applied only once in case of workers and pool registartion
		for (String attrName : setting.getModifiers().keySet()) {
			AttributeModifier<?> modifier = setting.getModifiers().get(attrName);
			try {
				rootNode.applyModifier(attrName, modifier, monitorables);
			} catch (ClassCastException cce) {
				throw new IllegalArgumentException("Cannot apply modifier \"" + modifier.getClass().getName() +
						"\" for attribute \"" + attrName + "\": " + cce.toString());
			}
		}

		MBeanInfo mBeanInfo = createMBeanInfo(rootNode, mbeanClass, isRefreshEnabled);
		Map<OperationKey, Method> opkeyToMethod = fetchOpkeyToMethod(mbeanClass);

		DynamicMBeanAggregator mbean = new DynamicMBeanAggregator(
				mBeanInfo, mbeanWrappers, rootNode, opkeyToMethod, isRefreshEnabled
		);

		// TODO(vmykhalko): maybe try to get all attributes and log warn message in case of exception? (to prevent potential errors during viewing jmx stats using jconsole)
//		tryGetAllAttributes(mbean);

		if (isRefreshEnabled) {
			handleJmxRefreshables(mbeanWrappers, rootNode);
		}
		return mbean;
	}

	// region building tree of AttributeNodes
	private List<AttributeNode> createNodesFor(Class<?> clazz, Class<?> mbeanClass,
											   String[] includedOptionalAttrs, Method getter) {

		Set<String> includedOptionals = new HashSet<>(asList(includedOptionalAttrs));
		List<AttributeDescriptor> attrDescriptors = fetchAttributeDescriptors(clazz);
		List<AttributeNode> attrNodes = new ArrayList<>();
		for (AttributeDescriptor descriptor : attrDescriptors) {
			check(descriptor.getGetter() != null, "@JmxAttribute \"%s\" does not have getter", descriptor.getName());

			String attrName;
			Method attrGetter = descriptor.getGetter();
			JmxAttribute attrAnnotation = attrGetter.getAnnotation(JmxAttribute.class);
			String attrAnnotationName = attrAnnotation.name();
			if (attrAnnotationName.equals(JmxAttribute.USE_GETTER_NAME)) {
				attrName = extractFieldNameFromGetter(attrGetter);
			} else {
				attrName = attrAnnotationName;
			}
			checkArgument(!attrName.contains("_"), "@JmxAttribute with name \"%s\" contains underscores", attrName);

			String attrDescription = null;
			if (!attrAnnotation.description().equals(JmxAttribute.NO_DESCRIPTION)) {
				attrDescription = attrAnnotation.description();
			}

			boolean included = !attrAnnotation.optional() || includedOptionals.contains(attrName);
			includedOptionals.remove(attrName);

			Type type = attrGetter.getGenericReturnType();
			Method attrSetter = descriptor.getSetter();
			AttributeNode attrNode = createAttributeNodeFor(attrName, attrDescription, type, included,
					attrAnnotation, attrGetter, attrSetter, mbeanClass);
			attrNodes.add(attrNode);
		}

		if (includedOptionals.size() > 0) {
			// in this case getter cannot be null
			throw new RuntimeException(format("Error in \"extraSubAttributes\" parameter in @JmxAnnotation" +
							" on %s.%s(). There is no field \"%s\" in %s.",
					getter.getDeclaringClass().getName(), getter.getName(),
					first(includedOptionals), getter.getReturnType().getName()));
		}

		return attrNodes;
	}

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
		Class<?> attrType = setter.getParameterTypes()[0];
		checkArgument(isSimpleType(attrType) || isAllowedType(attrType), "Setters are allowed only on SimpleType attributes."
				+ " But setter \"%s\" is not SimpleType setter", setter.getName());

		String name = extractFieldNameFromSetter(setter);

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

	@SuppressWarnings("unchecked")
	private AttributeNode createAttributeNodeFor(String attrName, String attrDescription, Type attrType,
												 boolean included,
												 JmxAttribute attrAnnotation,
												 Method getter, Method setter, Class<?> mbeanClass) {
		ValueFetcher defaultFetcher = getter != null ? new ValueFetcherFromGetter(getter) : new ValueFetcherDirect();
		if (attrType instanceof Class) {
			// 4 cases: custom-type, simple-type, JmxRefreshableStats, POJO
			Class<?> returnClass = (Class<?>) attrType;

			if (customTypes.containsKey(attrType)) {
				Transformer<?> transformer = customTypes.get(attrType);
				return new AttributeNodeForConverterType(attrName, attrDescription, included,
						defaultFetcher, setter, transformer.to, transformer.from);

			} else if (isSimpleType(returnClass)) {
				JmxReducer<?> reducer;
				try {
					reducer = fetchReducerFrom(getter);
				} catch (Exception e) {
					throw new RuntimeException(e);
				}
				return new AttributeNodeForSimpleType(
						attrName, attrDescription, included, defaultFetcher, setter, returnClass, reducer
				);

			} else if (isThrowable(returnClass)) {
				return new AttributeNodeForThrowable(attrName, attrDescription, included, defaultFetcher);

			} else if (returnClass.isArray()) {
				Class<?> elementType = returnClass.getComponentType();
				checkNotNull(getter, "Arrays can be used only directly in POJO, JmxRefreshableStats or JmxMBeans");
				ValueFetcher fetcher = new ValueFetcherFromGetterArrayAdapter(getter);
				return createListAttributeNodeFor(attrName, attrDescription, included, fetcher, elementType, mbeanClass);

			} else if (isJmxStats(returnClass)) {
				// JmxRefreshableStats case

				checkJmxStatsAreValid(returnClass, mbeanClass, getter);

				String[] extraSubAttributes =
						attrAnnotation != null ? attrAnnotation.extraSubAttributes() : new String[0];
				List<AttributeNode> subNodes =
						createNodesFor(returnClass, mbeanClass, extraSubAttributes, getter);

				if (subNodes.size() == 0) {
					throw new IllegalArgumentException(format(
							"JmxRefreshableStats of type \"%s\" does not have JmxAttributes",
							returnClass.getName()));
				}

				return new AttributeNodeForPojo(attrName, attrDescription, included, defaultFetcher,
						createReducerForJmxStats(returnClass), subNodes);

			} else {
				String[] extraSubAttributes =
						attrAnnotation != null ? attrAnnotation.extraSubAttributes() : new String[0];
				List<AttributeNode> subNodes =
						createNodesFor(returnClass, mbeanClass, extraSubAttributes, getter);

				if (subNodes.size() == 0) {
					// For unrecognized types just return standard toString
					// Note that they will be read-only
					return new AttributeNodeForConverterType<>(
							attrName, attrDescription,
							included, defaultFetcher,
							setter,
							Object::toString);
				} else {
					// POJO case

					JmxReducer<?> reducer;
					try {
						reducer = fetchReducerFrom(getter);
					} catch (Exception e) {
						throw new RuntimeException(e);
					}

					if (reducer.getClass() == JmxAttribute.DEFAULT_REDUCER) {
						return new AttributeNodeForPojo(
								attrName, attrDescription, included, defaultFetcher, null, subNodes);
					} else {
						return new AttributeNodeForPojo(attrName, attrDescription, included,
								defaultFetcher, reducer, subNodes);
					}
				}
			}
		} else if (attrType instanceof ParameterizedType) {
			return createNodeForParametrizedType(
					attrName, attrDescription, (ParameterizedType) attrType, included, getter, mbeanClass
			);
		} else {
			throw new RuntimeException();
		}
	}

	@SuppressWarnings("unchecked")
	private static JmxReducer<?> createReducerForJmxStats(Class<?> jmxStatsClass) {
		return (JmxReducer<Object>) sources -> {
			JmxStats accumulator;

			try {
				accumulator = createJmxAccumulator(jmxStatsClass);
			} catch (ReflectiveOperationException e) {
				throw new IllegalStateException("Cannot create JmxStats accumulator instance: " +
						jmxStatsClass.getName(), e);
			}

			for (Object pojo : sources) {
				JmxStats jmxStats = (JmxStats) pojo;
				if (jmxStats != null) {
					accumulator.add(jmxStats);
				}
			}

			return accumulator;
		};
	}

	private static JmxStats createJmxAccumulator(Class<?> jmxStatsClass) throws ReflectiveOperationException {
		assert JmxStats.class.isAssignableFrom(jmxStatsClass);

		if (ReflectionUtils.classHasPublicStaticFactoryMethod(jmxStatsClass, CREATE_ACCUMULATOR)) {
			return (JmxStats) jmxStatsClass.getDeclaredMethod(CREATE_ACCUMULATOR).invoke(null);
		} else if (ReflectionUtils.classHasPublicStaticFactoryMethod(jmxStatsClass, CREATE)) {
			return (JmxStats) jmxStatsClass.getDeclaredMethod(CREATE).invoke(null);
		} else if (ReflectionUtils.classHasPublicNoArgConstructor(jmxStatsClass)) {
			return (JmxStats) jmxStatsClass.newInstance();
		} else {
			throw new RuntimeException("Cannot create instance of class: " + jmxStatsClass.getName());
		}
	}

	private static JmxReducer<?> fetchReducerFrom(Method getter) throws IllegalAccessException, InstantiationException {
		if (getter == null) {
			return DEFAULT_REDUCER;
		} else {
			JmxAttribute attrAnnotation = getter.getAnnotation(JmxAttribute.class);
			Class<? extends JmxReducer<?>> reducerClass = attrAnnotation.reducer();
			if (reducerClass == DEFAULT_REDUCER.getClass()) {
				return DEFAULT_REDUCER;
			} else {
				return reducerClass.newInstance();
			}
		}
	}

	private static void checkJmxStatsAreValid(Class<?> returnClass, Class<?> mbeanClass, Method getter) {
		if (JmxRefreshableStats.class.isAssignableFrom(returnClass) &&
				!EventloopJmxMBean.class.isAssignableFrom(mbeanClass)) {
			logger.warn("JmxRefreshableStats won't be refreshed when used in classes that do not implement" +
					" EventloopJmxMBean. MBean class: " + mbeanClass.getName());
		}

		if (returnClass.isInterface()) {
			throw new IllegalArgumentException(createErrorMessageForInvalidJmxStatsAttribute(getter));
		}

		if (Modifier.isAbstract(returnClass.getModifiers())) {
			throw new IllegalArgumentException(createErrorMessageForInvalidJmxStatsAttribute(getter));
		}

		if (!(classHasPublicStaticFactoryMethod(returnClass, CREATE_ACCUMULATOR)
				|| classHasPublicStaticFactoryMethod(returnClass, CREATE)
				|| classHasPublicNoArgConstructor(returnClass))) {
			throw new IllegalArgumentException(createErrorMessageForInvalidJmxStatsAttribute(getter));
		}
	}

	private static String createErrorMessageForInvalidJmxStatsAttribute(Method getter) {
		String msg = "Return type of JmxRefreshableStats attribute must be concrete class that implements" +
				" JmxRefreshableStats interface and contains" +
				" static factory \"" + CREATE_ACCUMULATOR + "()\" method or" +
				" static factory \"" + CREATE + "()\" method or" +
				" public no-arg constructor";

		if (getter != null) {
			msg += format(". Error at %s.%s()", getter.getDeclaringClass().getName(), getter.getName());
		}
		return msg;
	}

	private AttributeNode createNodeForParametrizedType(String attrName, String attrDescription,
														ParameterizedType pType, boolean included,
														Method getter, Class<?> mbeanClass) {
		ValueFetcher fetcher = createAppropriateFetcher(getter);
		Class<?> rawType = (Class<?>) pType.getRawType();
		if (rawType == List.class) {
			Type listElementType = pType.getActualTypeArguments()[0];
			return createListAttributeNodeFor(
					attrName, attrDescription, included, fetcher, listElementType, mbeanClass);
		} else if (rawType == Map.class) {
			Type valueType = pType.getActualTypeArguments()[1];
			return createMapAttributeNodeFor(attrName, attrDescription, included, fetcher, valueType, mbeanClass);
		} else {
			throw new RuntimeException("There is no support for Generic classes other than List or Map");
		}
	}

	private AttributeNodeForList createListAttributeNodeFor(String attrName, String attrDescription,
															boolean included,
															ValueFetcher fetcher,
															Type listElementType, Class<?> mbeanClass) {
		if (listElementType instanceof Class<?>) {
			Class<?> listElementClass = (Class<?>) listElementType;
			boolean isListOfJmxRefreshable = (JmxRefreshable.class.isAssignableFrom(listElementClass));
			return new AttributeNodeForList(
					attrName,
					attrDescription,
					included,
					fetcher,
					createAttributeNodeFor("", attrDescription, listElementType, true, null, null, null, mbeanClass),
					isListOfJmxRefreshable
			);
		} else if (listElementType instanceof ParameterizedType) {
			String typeName = ((Class<?>) ((ParameterizedType) listElementType).getRawType()).getSimpleName();
			return new AttributeNodeForList(
					attrName,
					attrDescription,
					included,
					fetcher,
					createNodeForParametrizedType(
							typeName, attrDescription, (ParameterizedType) listElementType, true, null, mbeanClass
					),
					false
			);
		} else {
			throw new RuntimeException();
		}
	}

	private AttributeNodeForMap createMapAttributeNodeFor(String attrName, String attrDescription,
														  boolean included,
														  ValueFetcher fetcher,
														  Type valueType, Class<?> mbeanClass) {
		if (valueType instanceof Class<?>) {
			Class<?> valueClass = (Class<?>) valueType;
			boolean isMapOfJmxRefreshable = (JmxRefreshable.class.isAssignableFrom(valueClass));
			return new AttributeNodeForMap(
					attrName,
					attrDescription,
					included,
					fetcher,
					createAttributeNodeFor("", attrDescription, valueType, true, null, null, null, mbeanClass),
					isMapOfJmxRefreshable
			);
		} else if (valueType instanceof ParameterizedType) {
			String typeName = ((Class<?>) ((ParameterizedType) valueType).getRawType()).getSimpleName();
			return new AttributeNodeForMap(
					attrName,
					attrDescription,
					included,
					fetcher,
					createNodeForParametrizedType(
							typeName, attrDescription, (ParameterizedType) valueType, true, null, mbeanClass
					),
					false
			);
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
	// endregion

	// region refreshing jmx
	private void handleJmxRefreshables(List<MBeanWrapper> mbeanWrappers, AttributeNodeForPojo rootNode) {
		for (MBeanWrapper mbeanWrapper : mbeanWrappers) {
			Eventloop eventloop = mbeanWrapper.getEventloop();
			List<JmxRefreshable> currentRefreshables = rootNode.getAllRefreshables(mbeanWrapper.getMBean());
			if (!eventloopToJmxRefreshables.containsKey(eventloop)) {
				eventloopToJmxRefreshables.put(eventloop, currentRefreshables);
				eventloop.execute(createRefreshTask(eventloop, null, 0));
			} else {
				List<JmxRefreshable> previousRefreshables = eventloopToJmxRefreshables.get(eventloop);
				List<JmxRefreshable> allRefreshables = new ArrayList<>(previousRefreshables);
				allRefreshables.addAll(currentRefreshables);
				eventloopToJmxRefreshables.put(eventloop, allRefreshables);
			}

			refreshableStatsCounts.put(eventloop, eventloopToJmxRefreshables.get(eventloop).size());
		}
	}

	private Runnable createRefreshTask(Eventloop eventloop,
									   List<JmxRefreshable> previousList,
									   int previousRefreshes) {
		return () -> {
			long currentTime = eventloop.currentTimeMillis();

			List<JmxRefreshable> jmxRefreshableList = previousList;
			if (jmxRefreshableList == null) {
				// list might be updated in case of several mbeans in one eventloop
				jmxRefreshableList = eventloopToJmxRefreshables.get(eventloop);
				effectiveRefreshPeriods.put(eventloop, (int) computeEffectiveRefreshPeriod(jmxRefreshableList.size()));
			}

			int currentRefreshes = 0;
			while (currentRefreshes < maxJmxRefreshesPerOneCycle) {
				int index = currentRefreshes + previousRefreshes;
				if (index == jmxRefreshableList.size()) {
					break;
				}
				jmxRefreshableList.get(index).refresh(currentTime);
				currentRefreshes++;
			}

			long nextTimestamp = currentTime + computeEffectiveRefreshPeriod(jmxRefreshableList.size());
			int totalRefreshes = currentRefreshes + previousRefreshes;
			if (totalRefreshes == jmxRefreshableList.size()) {
				eventloop.scheduleBackground(nextTimestamp, createRefreshTask(eventloop, null, 0));
			} else {
				eventloop.scheduleBackground(nextTimestamp, createRefreshTask(eventloop, jmxRefreshableList, totalRefreshes));
			}
		};
	}

	private long computeEffectiveRefreshPeriod(int jmxRefreshablesCount) {
		if (jmxRefreshablesCount == 0) {
			return specifiedRefreshPeriod;
		}
		double ratio = ceil(jmxRefreshablesCount / (double) maxJmxRefreshesPerOneCycle);
		return (long) (specifiedRefreshPeriod / ratio);
	}

	private AttributeNodeForPojo createAttributesTree(Class<?> clazz) {
		List<AttributeNode> subNodes = createNodesFor(clazz, clazz, new String[0], null);
		return new AttributeNodeForPojo("", null, true, new ValueFetcherDirect(), null, subNodes);
	}
	// endregion

	// region creating jmx metadata - MBeanInfo
	private static MBeanInfo createMBeanInfo(AttributeNodeForPojo rootNode,
											 Class<?> monitorableClass,
											 boolean enableRefresh) {
		String monitorableName = "";
		String monitorableDescription = "";
		MBeanAttributeInfo[] attributes = rootNode != null ?
				fetchAttributesInfo(rootNode, enableRefresh) :
				new MBeanAttributeInfo[0];
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
		Set<String> visibleAttrs = rootNode.getVisibleAttributes();
		Map<String, OpenType<?>> nameToType = rootNode.getOpenTypes();
		Map<String, Map<String, String>> nameToDescriptions = rootNode.getDescriptions();
		List<MBeanAttributeInfo> attrsInfo = new ArrayList<>();
		for (String attrName : visibleAttrs) {
			String description = createDescription(attrName, nameToDescriptions.get(attrName));
			OpenType<?> attrType = nameToType.get(attrName);
			boolean writable = rootNode.isSettable(attrName);
			boolean isIs = attrType.equals(SimpleType.BOOLEAN);
			attrsInfo.add(new MBeanAttributeInfo(attrName, attrType.getClassName(), description, true, writable, isIs));
		}

		return attrsInfo.toArray(new MBeanAttributeInfo[attrsInfo.size()]);
	}

	private static String createDescription(String name, Map<String, String> groupDescriptions) {
		if (groupDescriptions.isEmpty()) {
			return name;
		}

		if (!name.contains("_")) {
			assert groupDescriptions.size() == 1;
			return first(groupDescriptions.values());
		}

		String descriptionTemplate = "\"%s\": %s";
		String separator = "  |  ";
		StringBuilder totalDescription = new StringBuilder("");
		for (String groupName : groupDescriptions.keySet()) {
			String groupDescription = groupDescriptions.get(groupName);
			totalDescription.append(String.format(descriptionTemplate, groupName, groupDescription));
			totalDescription.append(separator);
		}
		totalDescription.delete(totalDescription.length() - separator.length(), totalDescription.length());
		return totalDescription.toString();
	}

	private static MBeanOperationInfo[] fetchOperationsInfo(Class<?> monitorableClass, boolean enableRefresh) {
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
	// endregion

	// region jmx operations fetching
	private static Map<OperationKey, Method> fetchOpkeyToMethod(Class<?> mbeanClass) {
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
	// endregion

	// region etc
	private static int secondsToMillis(double seconds) {
		return (int) (seconds * 1000);
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
	// endregion

	// region helper classes
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
		private final List<? extends MBeanWrapper> mbeanWrappers;
		private final List<?> mbeans;
		private final AttributeNodeForPojo rootNode;
		private final Map<OperationKey, Method> opkeyToMethod;

		public DynamicMBeanAggregator(MBeanInfo mBeanInfo, List<? extends MBeanWrapper> mbeanWrappers,
									  AttributeNodeForPojo rootNode, Map<OperationKey, Method> opkeyToMethod,
									  boolean refreshEnabled) {
			this.mBeanInfo = mBeanInfo;
			this.mbeanWrappers = mbeanWrappers;

			List<Object> extractedMBeans = new ArrayList<>(mbeanWrappers.size());
			for (MBeanWrapper mbeanWrapper : mbeanWrappers) {
				extractedMBeans.add(mbeanWrapper.getMBean());
			}
			this.mbeans = extractedMBeans;

			this.rootNode = rootNode;
			this.opkeyToMethod = opkeyToMethod;
		}

		@SuppressWarnings("unchecked")
		@Override
		public Object getAttribute(String attribute)
				throws AttributeNotFoundException, MBeanException, ReflectionException {
			Object value = rootNode.aggregateAttributes(singleton(attribute), mbeans).get(attribute);
			if (value instanceof Throwable) {
				propagate((Throwable) value);
			}
			return value;
		}

		@Override
		public void setAttribute(Attribute attribute)
				throws AttributeNotFoundException, InvalidAttributeValueException, MBeanException, ReflectionException {
			String attrName = attribute.getName();
			Object attrValue = attribute.getValue();

			CountDownLatch latch = new CountDownLatch(mbeanWrappers.size());
			AtomicReference<Exception> exceptionReference = new AtomicReference<>();

			for (MBeanWrapper mbeanWrapper : mbeanWrappers) {
				Object mbean = mbeanWrapper.getMBean();
				mbeanWrapper.execute((new Runnable() {
					@Override
					public void run() {
						try {
							rootNode.setAttribute(attrName, attrValue, singletonList(mbean));
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
				Exception actualException = exception;
				if (exception instanceof SetterException) {
					SetterException setterException = (SetterException) exception;
					actualException = setterException.getCausedException();
				}
				propagate(actualException);
			}
		}

		@Override
		public AttributeList getAttributes(String[] attributes) {
			checkArgument(attributes != null);

			AttributeList attrList = new AttributeList();
			Set<String> attrNames = new HashSet<>(Arrays.asList(attributes));
			try {
				Map<String, Object> aggregatedAttrs = rootNode.aggregateAttributes(attrNames, mbeans);
				for (String aggregatedAttrName : aggregatedAttrs.keySet()) {
					Object aggregatedValue = aggregatedAttrs.get(aggregatedAttrName);
					if (!(aggregatedValue instanceof Throwable)) {
						attrList.add(new Attribute(aggregatedAttrName, aggregatedValue));
					}
				}
			} catch (Exception e) {
				logger.error("Cannot get attributes: " + attrNames, e);
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
					logger.error("Cannot set attribute: " + attribute.getName(), e);
				}
			}
			return resultList;
		}

		@Override
		public Object invoke(String actionName, Object[] params, String[] signature)
				throws MBeanException, ReflectionException {

			String[] argTypes = signature != null ? signature : new String[0];
			Object[] args = params != null ? params : new Object[0];
			OperationKey opkey = new OperationKey(actionName, argTypes);
			Method opMethod = opkeyToMethod.get(opkey);
			if (opMethod == null) {
				String operationName = prettyOperationName(actionName, argTypes);
				String errorMsg = "There is no operation \"" + operationName + "\"";
				throw new RuntimeOperationsException(new IllegalArgumentException("Operation not found"), errorMsg);
			}

			CountDownLatch latch = new CountDownLatch(mbeanWrappers.size());
			AtomicReference<Exception> exceptionReference = new AtomicReference<>();

			AtomicReference<Object> lastValue = new AtomicReference<>();
			for (MBeanWrapper mbeanWrapper : mbeanWrappers) {
				Object mbean = mbeanWrapper.getMBean();
				mbeanWrapper.execute((new Runnable() {
					@Override
					public void run() {
						try {
							Object result = opMethod.invoke(mbean, args);
							lastValue.set(result);
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
				propagate(exception);
			}

			// We don't know how to aggregate return values if there are several mbeans
			return mbeanWrappers.size() == 1 ? lastValue.get() : null;
		}

		private void propagate(Throwable throwable) throws MBeanException {
			if (throwable instanceof InvocationTargetException) {
				Throwable targetException = ((InvocationTargetException) throwable).getTargetException();

				if (targetException instanceof Exception) {
					throw new MBeanException((Exception) targetException);
				} else {
					throw new MBeanException(
							new Exception(format("Throwable of type \"%s\" and message \"%s\" " +
											"was thrown during method invocation",
									targetException.getClass().getName(), targetException.getMessage())
							)
					);
				}

			} else {
				if (throwable instanceof Exception) {
					throw new MBeanException((Exception) throwable);
				} else {
					throw new MBeanException(
							new Exception(format("Throwable of type \"%s\" and message \"%s\" " +
											"was thrown",
									throwable.getClass().getName(), throwable.getMessage())
							)
					);
				}
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

		@Override
		public MBeanInfo getMBeanInfo() {
			return mBeanInfo;
		}
	}

	private interface MBeanWrapper {
		void execute(Runnable command);

		Object getMBean();

		Eventloop getEventloop();
	}

	private static final class ConcurrentJmxMBeanWrapper implements MBeanWrapper {
		private final ConcurrentJmxMBean mbean;

		public ConcurrentJmxMBeanWrapper(ConcurrentJmxMBean mbean) {
			this.mbean = mbean;
		}

		@Override
		public void execute(Runnable command) {
			command.run();
		}

		@Override
		public Object getMBean() {
			return mbean;
		}

		@Override
		public Eventloop getEventloop() {
			return null;
		}
	}

	private static final class EventloopJmxMBeanWrapper implements MBeanWrapper {
		private final EventloopJmxMBean mbean;

		public EventloopJmxMBeanWrapper(EventloopJmxMBean mbean) {
			this.mbean = mbean;
		}

		@Override
		public void execute(Runnable command) {
			mbean.getEventloop().execute(command);
		}

		@Override
		public Object getMBean() {
			return mbean;
		}

		@Override
		public Eventloop getEventloop() {
			return mbean.getEventloop();
		}
	}

	static class Transformer<T> {
		@Nullable
		public final Function<String, T> from;
		public final Function<T, String> to;

		public Transformer(Function<T, String> to, Function<String, T> from) {
			this.to = to;
			this.from = from;
		}

		public Transformer(Function<T, String> to) {
			this.to = to;
			this.from = null;
		}

		public boolean isWritable() {
			return this.from != null;
		}
	}
	// endregion
}
