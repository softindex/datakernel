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

import io.datakernel.common.ref.Ref;
import io.datakernel.eventloop.Eventloop;
import io.datakernel.eventloop.jmx.EventloopJmxMBean;
import io.datakernel.eventloop.jmx.JmxRefreshable;
import io.datakernel.eventloop.jmx.JmxRefreshableStats;
import io.datakernel.eventloop.jmx.JmxStats;
import io.datakernel.eventloop.util.ReflectionUtils;
import io.datakernel.jmx.api.*;
import io.datakernel.jmx.api.JmxReducers.JmxReducerDistinct;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
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
import java.util.function.Function;

import static io.datakernel.common.Preconditions.checkArgument;
import static io.datakernel.common.Preconditions.checkNotNull;
import static io.datakernel.common.Utils.nullToDefault;
import static io.datakernel.common.collection.CollectionUtils.first;
import static io.datakernel.eventloop.RunnableWithContext.wrapContext;
import static io.datakernel.eventloop.util.ReflectionUtils.*;
import static io.datakernel.jmx.Utils.allInstancesAreOfSameType;
import static java.lang.Math.ceil;
import static java.lang.String.format;
import static java.util.Arrays.asList;
import static java.util.Collections.singleton;
import static java.util.Collections.singletonList;

@SuppressWarnings("rawtypes")
public final class DynamicMBeanFactoryImpl implements DynamicMBeanFactory {
	private static final Logger logger = LoggerFactory.getLogger(DynamicMBeanFactoryImpl.class);

	// refreshing jmx
	public static final Duration DEFAULT_REFRESH_PERIOD_IN_SECONDS = Duration.ofSeconds(1);
	public static final int MAX_JMX_REFRESHES_PER_ONE_CYCLE_DEFAULT = 500;
	private int maxJmxRefreshesPerOneCycle;
	private Duration specifiedRefreshPeriod;
	private final Map<Eventloop, List<JmxRefreshable>> eventloopToJmxRefreshables = new ConcurrentHashMap<>();
	private final Map<Eventloop, Integer> refreshableStatsCounts = new ConcurrentHashMap<>();
	private final Map<Eventloop, Integer> effectiveRefreshPeriods = new ConcurrentHashMap<>();

	private static final JmxReducer<?> DEFAULT_REDUCER = new JmxReducerDistinct();

	// JmxStats creator methods
	private static final String CREATE = "create";
	private static final String CREATE_ACCUMULATOR = "createAccumulator";

	private static final DynamicMBeanFactoryImpl INSTANCE_WITH_DEFAULT_REFRESH_PERIOD = new DynamicMBeanFactoryImpl(DEFAULT_REFRESH_PERIOD_IN_SECONDS, MAX_JMX_REFRESHES_PER_ONE_CYCLE_DEFAULT);

	// region constructor and factory methods
	private DynamicMBeanFactoryImpl(@NotNull Duration refreshPeriod, int maxJmxRefreshesPerOneCycle) {
		this.specifiedRefreshPeriod = refreshPeriod;
		this.maxJmxRefreshesPerOneCycle = maxJmxRefreshesPerOneCycle;
	}

	public static DynamicMBeanFactoryImpl create() {
		return INSTANCE_WITH_DEFAULT_REFRESH_PERIOD;
	}

	public static DynamicMBeanFactoryImpl create(Duration refreshPeriod, int maxJmxRefreshesPerOneCycle) {
		return new DynamicMBeanFactoryImpl(refreshPeriod, maxJmxRefreshesPerOneCycle);
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
		return specifiedRefreshPeriod;
	}

	public void setRefreshPeriod(Duration refreshPeriod) {
		this.specifiedRefreshPeriod = refreshPeriod;
	}

	public int getMaxJmxRefreshesPerOneCycle() {
		return maxJmxRefreshesPerOneCycle;
	}

	public void setMaxJmxRefreshesPerOneCycle(int maxJmxRefreshesPerOneCycle) {
		this.maxJmxRefreshesPerOneCycle = maxJmxRefreshesPerOneCycle;
	}
	// endregion

	/**
	 * Creates Jmx MBean for monitorables with operations and attributes.
	 */
	@Override
	public DynamicMBean createDynamicMBean(@NotNull List<?> monitorables, @NotNull MBeanSettings setting, boolean enableRefresh) {
		checkArgument(monitorables.size() > 0, "Size of list of monitorables should be greater than 0");
		checkArgument(monitorables.stream().noneMatch(Objects::isNull), "Monitorable can not be null");
		checkArgument(allInstancesAreOfSameType(monitorables), "Monitorables should be of the same type");

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

		AttributeNodeForPojo rootNode = createAttributesTree(mbeanClass, setting.getCustomTypes());
		rootNode.hideNullPojos(monitorables);

		for (String included : setting.getIncludedOptionals()) {
			rootNode.setVisible(included);
		}

		// TODO(vmykhalko): check in JmxRegistry that modifiers are applied only once in case of workers and pool registartion
		for (String attrName : setting.getModifiers().keySet()) {
			AttributeModifier<?> modifier = setting.getModifiers().get(attrName);
			try {
				rootNode.applyModifier(attrName, modifier, monitorables);
			} catch (ClassCastException e) {
				//noinspection ThrowInsideCatchBlockWhichIgnoresCaughtException - doesn't ignore
				throw new IllegalArgumentException("Cannot apply modifier \"" + modifier.getClass().getName() +
						"\" for attribute \"" + attrName + "\": " + e.toString());
			}
		}

		MBeanInfo mBeanInfo = createMBeanInfo(rootNode, mbeanClass);
		Map<OperationKey, Method> opkeyToMethod = fetchOpkeyToMethod(mbeanClass);

		DynamicMBeanAggregator mbean = new DynamicMBeanAggregator(mBeanInfo, mbeanWrappers, rootNode, opkeyToMethod);

		// TODO(vmykhalko): maybe try to get all attributes and log warn message in case of exception? (to prevent potential errors during viewing jmx stats using jconsole)
//		tryGetAllAttributes(mbean);

		if (isRefreshEnabled) {
			handleJmxRefreshables(mbeanWrappers, rootNode);
		}
		return mbean;
	}

	// region building tree of AttributeNodes
	private List<AttributeNode> createNodesFor(Class<?> clazz, Class<?> mbeanClass,
			String[] includedOptionalAttrs, @Nullable Method getter,
			Map<Type, JmxCustomTypeAdapter<?>> customTypes) {

		Set<String> includedOptionals = new HashSet<>(asList(includedOptionalAttrs));
		List<AttributeDescriptor> attrDescriptors = fetchAttributeDescriptors(clazz, customTypes);
		List<AttributeNode> attrNodes = new ArrayList<>();
		for (AttributeDescriptor descriptor : attrDescriptors) {
			checkNotNull(descriptor.getGetter(), "@JmxAttribute \"%s\" does not have getter", descriptor.getName());

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
					attrAnnotation, attrGetter, attrSetter, mbeanClass,
					customTypes);
			attrNodes.add(attrNode);
		}

		if (includedOptionals.size() > 0) {
			assert getter != null; // in this case getter cannot be null
			throw new RuntimeException(format("Error in \"extraSubAttributes\" parameter in @JmxAnnotation" +
							" on %s.%s(). There is no field \"%s\" in %s.",
					getter.getDeclaringClass().getName(), getter.getName(),
					first(includedOptionals), getter.getReturnType().getName()));
		}

		return attrNodes;
	}

	private List<AttributeDescriptor> fetchAttributeDescriptors(Class<?> clazz, Map<Type, JmxCustomTypeAdapter<?>> customTypes) {
		Map<String, AttributeDescriptor> nameToAttr = new HashMap<>();
		for (Method method : clazz.getMethods()) {
			if (method.isAnnotationPresent(JmxAttribute.class)) {
				if (isGetter(method)) {
					processGetter(nameToAttr, method);
				} else if (isSetter(method)) {
					processSetter(nameToAttr, method, customTypes);
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

			checkArgument(previousDescriptor.getGetter() == null,
					"More that one getter with name" + getter.getName());
			checkArgument(previousDescriptor.getType().equals(attrType),
					"Getter with name \"%s\" has different type than appropriate setter", getter.getName());

			nameToAttr.put(name, new AttributeDescriptor(name, attrType, getter, previousDescriptor.getSetter()));
		} else {
			nameToAttr.put(name, new AttributeDescriptor(name, attrType, getter, null));
		}
	}

	private void processSetter(Map<String, AttributeDescriptor> nameToAttr, Method setter, Map<Type, JmxCustomTypeAdapter<?>> customTypes) {
		Class<?> attrType = setter.getParameterTypes()[0];
		checkArgument(ReflectionUtils.isSimpleType(attrType) || customTypes.containsKey(attrType), "Setters are allowed only on SimpleType attributes."
				+ " But setter \"%s\" is not SimpleType setter", setter.getName());

		String name = extractFieldNameFromSetter(setter);

		if (nameToAttr.containsKey(name)) {
			AttributeDescriptor previousDescriptor = nameToAttr.get(name);

			checkArgument(previousDescriptor.getSetter() == null,
					"More that one setter with name" + setter.getName());
			checkArgument(previousDescriptor.getType().equals(attrType),
					"Setter with name \"%s\" has different type than appropriate getter", setter.getName());

			nameToAttr.put(name, new AttributeDescriptor(
					name, attrType, previousDescriptor.getGetter(), setter));
		} else {
			nameToAttr.put(name, new AttributeDescriptor(name, attrType, null, setter));
		}
	}

	@SuppressWarnings("unchecked")
	private AttributeNode createAttributeNodeFor(
			String attrName,
			@Nullable String attrDescription,
			Type attrType,
			boolean included,
			@Nullable JmxAttribute attrAnnotation,
			@Nullable Method getter,
			@Nullable Method setter,
			Class<?> mbeanClass,
			Map<Type, JmxCustomTypeAdapter<?>> customTypes) {
		if (attrType instanceof Class) {
			ValueFetcher defaultFetcher = createAppropriateFetcher(getter);
			// 4 cases: custom-type, simple-type, JmxRefreshableStats, POJO
			Class<? extends JmxStats> returnClass = (Class<? extends JmxStats>) attrType;

			if (customTypes.containsKey(attrType)) {
				JmxCustomTypeAdapter<?> customTypeAdapter = customTypes.get(attrType);
				return new AttributeNodeForConverterType(attrName, attrDescription, included,
						defaultFetcher, setter, customTypeAdapter.to, customTypeAdapter.from);

			} else if (ReflectionUtils.isSimpleType(returnClass)) {
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
				return createListAttributeNodeFor(attrName, attrDescription, included, fetcher, elementType, mbeanClass, customTypes);

			} else if (isJmxStats(returnClass)) {
				// JmxRefreshableStats case

				checkJmxStatsAreValid(returnClass, mbeanClass, getter);

				String[] extraSubAttributes =
						attrAnnotation != null ? attrAnnotation.extraSubAttributes() : new String[0];
				List<AttributeNode> subNodes =
						createNodesFor(returnClass, mbeanClass, extraSubAttributes, getter, customTypes);

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
						createNodesFor(returnClass, mbeanClass, extraSubAttributes, getter, customTypes);

				if (subNodes.size() == 0) {
					throw new IllegalArgumentException("Unrecognized type of Jmx attribute: " + attrType.getTypeName());
				} else {
					// POJO case

					JmxReducer<?> reducer;
					try {
						reducer = fetchReducerFrom(getter);
					} catch (Exception e) {
						throw new RuntimeException(e);
					}

					return new AttributeNodeForPojo(
							attrName, attrDescription, included, defaultFetcher, reducer == DEFAULT_REDUCER ? null : reducer, subNodes);
				}
			}
		} else if (attrType instanceof ParameterizedType) {
			return createNodeForParametrizedType(
					attrName, attrDescription, (ParameterizedType) attrType, included, getter, setter, mbeanClass, customTypes);
		} else {
			throw new IllegalArgumentException("Unrecognized type of Jmx attribute: " + attrType.getTypeName());
		}
	}

	@SuppressWarnings("unchecked")
	private static JmxReducer<?> createReducerForJmxStats(Class<? extends JmxStats> jmxStatsClass) {
		return (JmxReducer<Object>) sources -> {
			JmxStats accumulator = createJmxAccumulator(jmxStatsClass);
			for (Object pojo : sources) {
				JmxStats jmxStats = (JmxStats) pojo;
				if (jmxStats != null) {
					accumulator.add(jmxStats);
				}
			}
			return accumulator;
		};
	}

	private static JmxStats createJmxAccumulator(Class<? extends JmxStats> jmxStatsClass) {
		JmxStats jmxStats = ReflectionUtils.tryToCreateInstanceWithFactoryMethods(jmxStatsClass, CREATE_ACCUMULATOR, CREATE);
		if (jmxStats == null) {
			throw new RuntimeException("Cannot create JmxStats accumulator instance: " + jmxStatsClass.getName());
		}
		return jmxStats;
	}

	@SuppressWarnings("unchecked")
	private static JmxReducer<?> fetchReducerFrom(@Nullable Method getter) throws IllegalAccessException, InstantiationException {
		if (getter == null) {
			return DEFAULT_REDUCER;
		}
		JmxAttribute attrAnnotation = getter.getAnnotation(JmxAttribute.class);
		Class<?> reducerClass = attrAnnotation.reducer();
		if (reducerClass == DEFAULT_REDUCER.getClass()) {
			return DEFAULT_REDUCER;
		}
		return ((Class<? extends JmxReducer<?>>) reducerClass).newInstance();
	}

	private static void checkJmxStatsAreValid(Class<?> returnClass, Class<?> mbeanClass, @Nullable Method getter) {
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

		if (!canBeCreated(returnClass, CREATE_ACCUMULATOR, CREATE)) {
			throw new IllegalArgumentException(createErrorMessageForInvalidJmxStatsAttribute(getter));
		}
	}

	private static String createErrorMessageForInvalidJmxStatsAttribute(@Nullable Method getter) {
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

	private AttributeNode createNodeForParametrizedType(String attrName, @Nullable String attrDescription,
			ParameterizedType pType, boolean included,
			@Nullable Method getter, @Nullable Method setter, Class<?> mbeanClass,
			Map<Type, JmxCustomTypeAdapter<?>> customTypes) {
		ValueFetcher fetcher = createAppropriateFetcher(getter);
		Class<?> rawType = (Class<?>) pType.getRawType();

		if (rawType == List.class) {
			Type listElementType = pType.getActualTypeArguments()[0];
			return createListAttributeNodeFor(
					attrName, attrDescription, included, fetcher, listElementType, mbeanClass, customTypes);
		} else if (rawType == Map.class) {
			Type valueType = pType.getActualTypeArguments()[1];
			return createMapAttributeNodeFor(attrName, attrDescription, included, fetcher, valueType, mbeanClass, customTypes);
		} else if (customTypes.containsKey(rawType)) {
			return createConverterAttributeNodeFor(attrName, attrDescription, pType, included, fetcher, setter, customTypes);
		} else {
			throw new IllegalArgumentException("There is no support for generic class " + pType.getTypeName());
		}
	}

	@SuppressWarnings("unchecked")
	private AttributeNodeForConverterType createConverterAttributeNodeFor(
			String attrName,
			@Nullable String attrDescription,
			ParameterizedType type, boolean included,
			ValueFetcher fetcher, @Nullable Method setter,
			Map<Type, JmxCustomTypeAdapter<?>> customTypes) {
		Type[] actualTypes = type.getActualTypeArguments();
		for (Type genericType : actualTypes) {
			if (!customTypes.containsKey(genericType)) {
				throw new IllegalArgumentException("There is no support for generic type " + type.getTypeName());
			}
		}
		JmxCustomTypeAdapter<?> t = customTypes.get(type.getRawType());
		return new AttributeNodeForConverterType(attrName, attrDescription, fetcher, included, setter, t.to, t.from);
	}

	private AttributeNodeForList createListAttributeNodeFor(
			String attrName,
			@Nullable String attrDescription,
			boolean included,
			ValueFetcher fetcher,
			Type listElementType, Class<?> mbeanClass,
			Map<Type, JmxCustomTypeAdapter<?>> customTypes) {
		if (listElementType instanceof Class<?>) {
			Class<?> listElementClass = (Class<?>) listElementType;
			boolean isListOfJmxRefreshable = JmxRefreshable.class.isAssignableFrom(listElementClass);
			return new AttributeNodeForList(
					attrName,
					attrDescription,
					included,
					fetcher,
					createAttributeNodeFor("", attrDescription, listElementType, true, null, null, null, mbeanClass, customTypes),
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
							typeName, attrDescription, (ParameterizedType) listElementType, true, null, null, mbeanClass,
							customTypes
					),
					false
			);
		} else {
			throw new IllegalArgumentException("Can't create list attribute node for List<" + listElementType.getTypeName() + ">");
		}
	}

	private AttributeNodeForMap createMapAttributeNodeFor(
			String attrName,
			@Nullable String attrDescription,
			boolean included, ValueFetcher fetcher,
			Type valueType, Class<?> mbeanClass,
			Map<Type, JmxCustomTypeAdapter<?>> customTypes) {
		boolean isMapOfJmxRefreshable = false;
		AttributeNode node;
		if (valueType instanceof Class<?>) {
			Class<?> valueClass = (Class<?>) valueType;
			isMapOfJmxRefreshable = JmxRefreshable.class.isAssignableFrom(valueClass);
			node = createAttributeNodeFor("", attrDescription, valueType, true, null, null, null, mbeanClass, customTypes);
		} else if (valueType instanceof ParameterizedType) {
			String typeName = ((Class<?>) ((ParameterizedType) valueType).getRawType()).getSimpleName();
			node = createNodeForParametrizedType(typeName, attrDescription, (ParameterizedType) valueType, true, null, null, mbeanClass,
					customTypes);
		} else {
			throw new IllegalArgumentException("Can't create map attribute node for " + valueType.getTypeName());
		}
		return new AttributeNodeForMap(attrName, attrDescription, included, fetcher, node, isMapOfJmxRefreshable);
	}

	private static ValueFetcher createAppropriateFetcher(@Nullable Method getter) {
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
				eventloop.execute(wrapContext(this, createRefreshTask(eventloop, null, 0)));
			} else {
				List<JmxRefreshable> previousRefreshables = eventloopToJmxRefreshables.get(eventloop);
				List<JmxRefreshable> allRefreshables = new ArrayList<>(previousRefreshables);
				allRefreshables.addAll(currentRefreshables);
				eventloopToJmxRefreshables.put(eventloop, allRefreshables);
			}

			refreshableStatsCounts.put(eventloop, eventloopToJmxRefreshables.get(eventloop).size());
		}
	}

	private Runnable createRefreshTask(Eventloop eventloop, @Nullable List<JmxRefreshable> previousList, int previousRefreshes) {
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
				eventloop.scheduleBackground(nextTimestamp, wrapContext(this, createRefreshTask(eventloop, null, 0)));
			} else {
				eventloop.scheduleBackground(nextTimestamp, wrapContext(this, createRefreshTask(eventloop, jmxRefreshableList, totalRefreshes)));
			}
		};
	}

	private long computeEffectiveRefreshPeriod(int jmxRefreshablesCount) {
		if (jmxRefreshablesCount == 0) {
			return specifiedRefreshPeriod.toMillis();
		}
		double ratio = ceil(jmxRefreshablesCount / (double) maxJmxRefreshesPerOneCycle);
		return (long) (specifiedRefreshPeriod.toMillis() / ratio);
	}

	/**
	 * Creates attribute tree of Jmx attributes for clazz.
	 */
	private AttributeNodeForPojo createAttributesTree(Class<?> clazz, Map<Type, JmxCustomTypeAdapter<?>> customTypes) {
		List<AttributeNode> subNodes = createNodesFor(clazz, clazz, new String[0], null, customTypes);
		return new AttributeNodeForPojo("", null, true, new ValueFetcherDirect(), null, subNodes);
	}
	// endregion

	// region creating jmx metadata - MBeanInfo
	private static MBeanInfo createMBeanInfo(AttributeNodeForPojo rootNode, Class<?> monitorableClass) {
		String monitorableName = "";
		String monitorableDescription = "";
		MBeanAttributeInfo[] attributes = rootNode != null ?
				fetchAttributesInfo(rootNode) :
				new MBeanAttributeInfo[0];
		MBeanOperationInfo[] operations = fetchOperationsInfo(monitorableClass);
		return new MBeanInfo(
				monitorableName,
				monitorableDescription,
				attributes,
				null,  // constructors
				operations,
				null); //notifications
	}

	private static MBeanAttributeInfo[] fetchAttributesInfo(AttributeNodeForPojo rootNode) {
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

		return attrsInfo.toArray(new MBeanAttributeInfo[0]);
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
		StringBuilder totalDescription = new StringBuilder();
		for (String groupName : groupDescriptions.keySet()) {
			String groupDescription = groupDescriptions.get(groupName);
			totalDescription.append(String.format(descriptionTemplate, groupName, groupDescription));
			totalDescription.append(separator);
		}
		totalDescription.delete(totalDescription.length() - separator.length(), totalDescription.length());
		return totalDescription.toString();
	}

	private static MBeanOperationInfo[] fetchOperationsInfo(Class<?> monitorableClass) {
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
				MBeanParameterInfo[] paramsArray = params.toArray(new MBeanParameterInfo[0]);
				MBeanOperationInfo operationInfo = new MBeanOperationInfo(
						opName, opDescription, paramsArray, returnType.getName(), MBeanOperationInfo.ACTION);
				operations.add(operationInfo);
			}
		}

		return operations.toArray(new MBeanOperationInfo[0]);
	}

	@Nullable
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

	// region helper classes
	private static final class AttributeDescriptor {
		private final String name;
		private final Type type;
		private final Method getter;
		private final Method setter;

		public AttributeDescriptor(String name, Type type, @Nullable Method getter, @Nullable Method setter) {
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

		@Nullable
		public Method getGetter() {
			return getter;
		}

		@Nullable
		public Method getSetter() {
			return setter;
		}
	}

	private static final class OperationKey {
		private final String name;
		private final String[] argTypes;

		public OperationKey(@NotNull String name, @NotNull String[] argTypes) {
			this.name = name;
			this.argTypes = argTypes;
		}

		public String getName() {
			return name;
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
				AttributeNodeForPojo rootNode, Map<OperationKey, Method> opkeyToMethod) {
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

		@Override
		public Object getAttribute(String attribute) throws MBeanException {
			Object value = rootNode.aggregateAttributes(singleton(attribute), mbeans).get(attribute);
			if (value instanceof Throwable) {
				propagate((Throwable) value);
			}
			return value;
		}

		@Override
		public void setAttribute(Attribute attribute)
				throws MBeanException {
			String attrName = attribute.getName();
			Object attrValue = attribute.getValue();

			CountDownLatch latch = new CountDownLatch(mbeanWrappers.size());
			Ref<Exception> exceptionRef = new Ref<>();

			for (MBeanWrapper mbeanWrapper : mbeanWrappers) {
				Object mbean = mbeanWrapper.getMBean();
				mbeanWrapper.execute(() -> {
					try {
						rootNode.setAttribute(attrName, attrValue, singletonList(mbean));
						latch.countDown();
					} catch (Exception e) {
						exceptionRef.set(e);
						latch.countDown();
					}
				});
			}

			try {
				latch.await();
			} catch (InterruptedException e) {
				throw new MBeanException(e);
			}

			Exception e = exceptionRef.get();
			if (e != null) {
				Exception actualException = e;
				if (e instanceof SetterException) {
					SetterException setterException = (SetterException) e;
					actualException = setterException.getCausedException();
				}
				propagate(actualException);
			}
		}

		@Override
		public AttributeList getAttributes(@NotNull String[] attributes) {
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
			for (Object attr : attributes) {
				Attribute attribute = (Attribute) attr;
				try {
					setAttribute(attribute);
					resultList.add(new Attribute(attribute.getName(), attribute.getValue()));
				} catch (MBeanException e) {
					logger.error("Cannot set attribute: " + attribute.getName(), e);
				}
			}
			return resultList;
		}

		@Nullable
		@Override
		public Object invoke(String actionName, Object[] params, String[] signature)
				throws MBeanException {

			Object[] args = nullToDefault(params, new Object[0]);
			String[] argTypes = nullToDefault(signature, new String[0]);
			OperationKey opkey = new OperationKey(actionName, argTypes);
			Method opMethod = opkeyToMethod.get(opkey);
			if (opMethod == null) {
				String operationName = prettyOperationName(actionName, argTypes);
				String errorMsg = "There is no operation \"" + operationName + "\"";
				throw new RuntimeOperationsException(new IllegalArgumentException("Operation not found"), errorMsg);
			}

			CountDownLatch latch = new CountDownLatch(mbeanWrappers.size());
			Ref<Object> lastValueRef = new Ref<>();
			Ref<Exception> exceptionRef = new Ref<>();
			for (MBeanWrapper mbeanWrapper : mbeanWrappers) {
				Object mbean = mbeanWrapper.getMBean();
				mbeanWrapper.execute(() -> {
					try {
						Object result = opMethod.invoke(mbean, args);
						lastValueRef.set(result);
						latch.countDown();
					} catch (Exception e) {
						exceptionRef.set(e);
						latch.countDown();
					}
				});
			}

			try {
				latch.await();
			} catch (InterruptedException e) {
				throw new MBeanException(e);
			}

			Exception e = exceptionRef.get();
			if (e != null) {
				propagate(e);
			}

			// We don't know how to aggregate return values if there are several mbeans
			return mbeanWrappers.size() == 1 ? lastValueRef.get() : null;
		}

		private void propagate(Throwable e) throws MBeanException {
			if (e instanceof InvocationTargetException) {
				Throwable targetException = ((InvocationTargetException) e).getTargetException();

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
				if (e instanceof Exception) {
					throw new MBeanException((Exception) e);
				} else {
					throw new MBeanException(
							new Exception(format("Throwable of type \"%s\" and message \"%s\" " +
											"was thrown",
									e.getClass().getName(), e.getMessage())
							)
					);
				}
			}
		}

		private static String prettyOperationName(String name, String[] argTypes) {
			StringBuilder operationName = new StringBuilder(name).append('(');
			if (argTypes.length > 0) {
				for (int i = 0; i < argTypes.length - 1; i++) {
					operationName.append(argTypes[i]).append(", ");
				}
				operationName.append(argTypes[argTypes.length - 1]);
			}
			operationName.append(")");
			return operationName.toString();
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

		@Nullable
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
			mbean.getEventloop().execute(wrapContext(mbean, command));
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

	static class JmxCustomTypeAdapter<T> {
		@Nullable
		public final Function<String, T> from;
		public final Function<T, String> to;

		public JmxCustomTypeAdapter(Function<T, String> to, @Nullable Function<String, T> from) {
			this.to = to;
			this.from = from;
		}

		public JmxCustomTypeAdapter(Function<T, String> to) {
			this.to = to;
			this.from = null;
		}
	}
	// endregion
}
