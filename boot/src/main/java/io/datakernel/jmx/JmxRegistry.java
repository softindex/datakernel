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

import com.google.inject.Key;
import io.datakernel.jmx.JmxMBeans.JmxCustomTypeAdapter;
import io.datakernel.worker.WorkerPool;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.management.*;
import java.lang.annotation.Annotation;
import java.lang.reflect.Method;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.util.*;

import static io.datakernel.util.Preconditions.checkNotNull;
import static io.datakernel.util.StringFormatUtils.formatDuration;
import static io.datakernel.util.StringFormatUtils.parseDuration;
import static java.lang.String.format;
import static java.util.Collections.singletonList;
import static java.util.stream.Collectors.joining;

public final class JmxRegistry implements JmxRegistryMXBean {
	private final Logger logger = LoggerFactory.getLogger(this.getClass());

	private static final String GENERIC_PARAM_NAME_FORMAT = "T%d=%s";

	private final MBeanServer mbs;
	private final DynamicMBeanFactory mbeanFactory;
	private final Map<Key<?>, String> keyToObjectNames;
	private final Map<Type, JmxCustomTypeAdapter<?>> customTypes;

	// jmx
	private int registeredSingletons;
	private int registeredPools;
	private int totallyRegisteredMBeans;

	private JmxRegistry(MBeanServer mbs,
			DynamicMBeanFactory mbeanFactory,
			Map<Key<?>, String> keyToObjectNames,
			Map<Type, JmxCustomTypeAdapter<?>> customTypes) {
		this.mbs = checkNotNull(mbs);
		this.mbeanFactory = checkNotNull(mbeanFactory);
		this.keyToObjectNames = keyToObjectNames;
		this.customTypes = customTypes;
	}

	public static JmxRegistry create(MBeanServer mbs, DynamicMBeanFactory mbeanFactory) {
		return new JmxRegistry(mbs, mbeanFactory, Collections.emptyMap(), Collections.emptyMap());
	}

	public static JmxRegistry create(MBeanServer mbs,
			DynamicMBeanFactory mbeanFactory,
			Map<Key<?>, String> keyToObjectNames,
			Map<Type, JmxCustomTypeAdapter<?>> customTypes) {
		return new JmxRegistry(mbs, mbeanFactory, keyToObjectNames, customTypes);
	}

	public void registerSingleton(Key<?> key, Object singletonInstance, MBeanSettings settings) {
		checkNotNull(singletonInstance);
		checkNotNull(key);

		Class<?> instanceClass = singletonInstance.getClass();
		Object mbean;
		if (isJmxMBean(instanceClass)) {
			// this will throw exception if something happens during initialization
			mbean = mbeanFactory.createFor(singletonList(singletonInstance), settings, true);
		} else if (isStandardMBean(instanceClass) || isMXBean(instanceClass) || isDynamicMBean(instanceClass)) {
			mbean = singletonInstance;
		} else {
			logger.trace(format("Instance with key %s was not registered to jmx, " +
					"because its type does not implement ConcurrentJmxMBean, EventloopJmxMBean " +
					"and does not implement neither *MBean nor *MXBean interface", key.toString()));
			return;
		}

		String name;
		try {
			name = createNameForKey(key);
		} catch (ReflectiveOperationException e) {
			String msg = format("Error during generation name for instance with key %s", key.toString());
			logger.error(msg, e);
			return;
		}

		ObjectName objectName;
		try {
			objectName = new ObjectName(name);
		} catch (MalformedObjectNameException e) {
			String msg = format("Cannot create ObjectName for instance with key %s. " +
					"Proposed String name was \"%s\".", key.toString(), name);
			logger.error(msg, e);
			return;
		}

		try {
			mbs.registerMBean(mbean, objectName);
			logger.trace(format("Instance with key %s was successfully registered to jmx " +
					"with ObjectName \"%s\" ", key.toString(), objectName.toString()));

			registeredSingletons++;
			totallyRegisteredMBeans++;

		} catch (NotCompliantMBeanException | InstanceAlreadyExistsException | MBeanRegistrationException e) {
			String msg = format("Cannot register MBean for instance with key %s and ObjectName \"%s\"",
					key.toString(), objectName.toString());
			logger.error(msg, e);
			return;
		}
	}

	public void unregisterSingleton(Key<?> key, Object singletonInstance) {
		checkNotNull(key);

		if (isMBean(singletonInstance.getClass())) {
			try {
				String name = createNameForKey(key);
				ObjectName objectName = new ObjectName(name);
				mbs.unregisterMBean(objectName);
			} catch (ReflectiveOperationException | JMException e) {
				String msg =
						format("Error during attempt to unregister MBean for instance with key %s.", key.toString());
				logger.error(msg, e);
			}
		}
	}

	public void registerWorkers(WorkerPool pool, Key<?> key, List<?> poolInstances, MBeanSettings settings) {
		checkNotNull(poolInstances);
		checkNotNull(key);

		if (poolInstances.size() == 0) {
			logger.info(format("Pool of instances with key %s is empty", key.toString()));
			return;
		}

		if (!allInstancesAreOfSameType(poolInstances)) {
			logger.info(format("Pool of instances with key %s was not registered to jmx because their types differ", key.toString()));
			return;
		}

		if (!isJmxMBean(poolInstances.get(0).getClass())) {
			logger.info(format("Pool of instances with key %s was not registered to jmx, " +
					"because instances' type implements neither ConcurrentJmxMBean " +
					"nor EventloopJmxMBean interface", key.toString()));
			return;
		}

		String commonName;
		try {
			commonName = createNameForKey(key);
		} catch (Exception e) {
			String msg = format("Error during generation name for pool of instances with key %s", key.toString());
			logger.error(msg, e);
			return;
		}

		// register mbeans for each worker separately
		for (int i = 0; i < poolInstances.size(); i++) {
			MBeanSettings settingsForOptionals = MBeanSettings.of(
					settings.getIncludedOptionals(), new HashMap<>(), customTypes);
			registerMBeanForWorker(poolInstances.get(i), pool, i, commonName, key, settingsForOptionals);
		}

		// register aggregated mbean for pool of workers
		DynamicMBean mbean;
		try {
			mbean = mbeanFactory.createFor(poolInstances, settings, true);
		} catch (Exception e) {
			String msg = format("Cannot create DynamicMBean for aggregated MBean of pool of workers with key %s",
					key.toString());
			logger.error(msg, e);
			return;
		}

		ObjectName objectName;
		try {
			objectName = new ObjectName(commonName);
		} catch (MalformedObjectNameException e) {
			String msg = format("Cannot create ObjectName for aggregated MBean of pool of workers with key %s. " +
					"Proposed String name was \"%s\".", key.toString(), commonName);
			logger.error(msg, e);
			return;
		}

		try {
			mbs.registerMBean(mbean, objectName);
			logger.trace(format("Pool of instances with key %s was successfully registered to jmx " +
					"with ObjectName \"%s\"", key.toString(), objectName.toString()));

			registeredPools++;
			totallyRegisteredMBeans++;

		} catch (NotCompliantMBeanException | InstanceAlreadyExistsException | MBeanRegistrationException e) {
			String msg = format("Cannot register aggregated MBean of pool of workers with key %s " +
					"and ObjectName \"%s\"", key.toString(), objectName.toString());
			logger.error(msg, e);
			return;
		}
	}

	public void unregisterWorkers(WorkerPool pool, Key<?> key, List<?> poolInstances) {
		checkNotNull(key);

		if (poolInstances.size() == 0) {
			return;
		}

		if (!allInstancesAreOfSameType(poolInstances)) {
			return;
		}

		if (!isJmxMBean(poolInstances.get(0).getClass())) {
			return;
		}

		String commonName;
		try {
			commonName = createNameForKey(key);
		} catch (ReflectiveOperationException e) {
			String msg = format("Error during generation name for pool of instances with key %s", key.toString());
			logger.error(msg, e);
			return;
		}

		// unregister mbeans for each worker separately
		for (int i = 0; i < poolInstances.size(); i++) {
			try {
				String workerName = createWorkerName(commonName, i, pool);
				mbs.unregisterMBean(new ObjectName(workerName));
			} catch (JMException e) {
				String msg = format("Error during attempt to unregister mbean for worker" +
								" of pool of instances with key %s. Worker id is \"%d\"",
						key.toString(), i);
				logger.error(msg, e);
			}
		}

		// unregister aggregated mbean for pool of workers
		try {
			mbs.unregisterMBean(new ObjectName(commonName));
		} catch (JMException e) {
			String msg = format("Error during attempt to unregister aggregated mbean for pool of instances " +
					"with key %s.", key.toString());
			logger.error(msg, e);
		}
	}

	private boolean allInstancesAreOfSameType(List<?> instances) {
		int last = instances.size() - 1;
		for (int i = 0; i < last; i++) {
			if (!instances.get(i).getClass().equals(instances.get(i + 1).getClass())) {
				return false;
			}
		}
		return true;
	}

	private void registerMBeanForWorker(Object worker, WorkerPool pool, int workerId, String commonName,
			Key<?> key, MBeanSettings settings) {
		String workerName = createWorkerName(commonName, workerId, pool);

		DynamicMBean mbean;
		try {
			mbean = mbeanFactory.createFor(singletonList(worker), settings, false);
		} catch (Exception e) {
			String msg = format("Cannot create DynamicMBean for worker " +
					"of pool of instances with key %s", key.toString());
			logger.error(msg, e);
			return;
		}

		ObjectName objectName;
		try {
			objectName = new ObjectName(workerName);
		} catch (MalformedObjectNameException e) {
			String msg = format("Cannot create ObjectName for worker of pool of instances with key %s. " +
					"Proposed String name was \"%s\".", key.toString(), workerName);
			logger.error(msg, e);
			return;
		}

		try {
			mbs.registerMBean(mbean, objectName);

			totallyRegisteredMBeans++;

		} catch (NotCompliantMBeanException | InstanceAlreadyExistsException | MBeanRegistrationException e) {
			String msg = format("Cannot register MBean for worker of pool of instances with key %s. " +
					"ObjectName for worker is \"%s\"", key.toString(), objectName.toString());
			logger.error(msg, e);
			return;
		}
	}

	private static String createWorkerName(String commonName, int workerId, WorkerPool workerPool) {
		return commonName + format(",workerId=worker-%d,workerPool=%s", workerId, workerPool.toString());
	}

	private String createNameForKey(Key<?> key) throws ReflectiveOperationException {
		if (keyToObjectNames.containsKey(key))
			return keyToObjectNames.get(key);
		Class<?> rawType = key.getTypeLiteral().getRawType();
		Annotation annotation = key.getAnnotation();
		String domain = rawType.getPackage().getName();
		String name = domain + ":";
		if (annotation == null) { // without annotation
			name += "type=" + rawType.getSimpleName();
		} else {
			Class<? extends Annotation> annotationType = annotation.annotationType();
			Method[] annotationElements = filterNonEmptyElements(annotation);
			name += "type=" + rawType.getSimpleName() + ",";
			if (annotationElements.length == 0) { // annotation without elements
				name += "annotation=" + annotationType.getSimpleName();
			} else if (annotationElements.length == 1 && annotationElements[0].getName().equals("value")) {
				// annotation with single element which has name "value"
				Object value = fetchAnnotationElementValue(annotation, annotationElements[0]);
				name += annotationType.getSimpleName() + "=" + value.toString();
			} else { // annotation with one or more custom elements
				for (Method annotationParameter : annotationElements) {
					Object value = fetchAnnotationElementValue(annotation, annotationParameter);
					String nameKey = annotationParameter.getName();
					String nameValue = value.toString();
					name += nameKey + "=" + nameValue + ",";
				}

				assert name.substring(name.length() - 1).equals(",");

				name = name.substring(0, name.length() - 1);
			}
		}
		return addGenericParamsInfo(name, key);
	}

	private static String addGenericParamsInfo(String srcName, Key<?> key) {
		Type type = key.getTypeLiteral().getType();
		String resultName = srcName;
		if (type instanceof ParameterizedType) {
			ParameterizedType pType = (ParameterizedType) type;
			Type[] genericArgs = pType.getActualTypeArguments();
			for (int i = 0; i < genericArgs.length; i++) {
				Type genericArg = genericArgs[i];
				String argClassName = formatSimpleGenericName(genericArg);
				int argId = i + 1;
				resultName += "," + format(GENERIC_PARAM_NAME_FORMAT, argId, argClassName);
			}
		}
		return resultName;
	}

	private static String formatSimpleGenericName(Type type) {
		if (type instanceof Class) {
			return ((Class<?>) type).getSimpleName();
		}
		ParameterizedType genericType = (ParameterizedType) type;
		return ((Class<?>) genericType.getRawType()).getSimpleName() +
				Arrays.stream(genericType.getActualTypeArguments())
						.map(JmxRegistry::formatSimpleGenericName)
						.collect(joining(";", "<", ">"));
	}

	/**
	 * Returns values if it is not null, otherwise throws exception
	 */
	private static Object fetchAnnotationElementValue(Annotation annotation, Method element) throws ReflectiveOperationException {
		Object value = element.invoke(annotation);
		if (value == null) {
			String errorMsg = "@" + annotation.annotationType().getName() + "." +
					element.getName() + "() returned null";
			throw new NullPointerException(errorMsg);
		}
		return value;
	}

	private static Method[] filterNonEmptyElements(Annotation annotation) throws ReflectiveOperationException {
		List<Method> filtered = new ArrayList<>();
		for (Method method : annotation.annotationType().getDeclaredMethods()) {
			Object elementValue = fetchAnnotationElementValue(annotation, method);
			if (elementValue instanceof String) {
				String stringValue = (String) elementValue;
				if (stringValue.length() == 0) {
					// skip this element, because it is empty string
					continue;
				}
			}
			filtered.add(method);
		}
		return filtered.toArray(new Method[filtered.size()]);
	}

	private static boolean isStandardMBean(Class<?> clazz) {
		return classImplementsInterfaceWithNameEndingWith(clazz, "MBean");
	}

	private static boolean isMXBean(Class<?> clazz) {
		return classFollowsMXBeanConvention(clazz);
	}

	private static boolean classImplementsInterfaceWithNameEndingWith(Class<?> clazz, String ending) {
		String clazzName = clazz.getSimpleName();
		Class<?>[] interfaces = clazz.getInterfaces();
		for (Class<?> anInterface : interfaces) {
			String interfaceName = anInterface.getSimpleName();
			if (interfaceName.equals(clazzName + ending)) {
				return true;
			}
		}
		return false;
	}

	private static boolean classFollowsMXBeanConvention(Class<?> clazz) {
		Class<?>[] interfazes = clazz.getInterfaces();
		for (Class<?> interfaze : interfazes) {
			if (interfaceFollowsMXBeanConvention(interfaze)) {
				return true;
			}
		}

		Class<?> superClazz = clazz.getSuperclass();
		if (superClazz != null) {
			return classFollowsMXBeanConvention(superClazz);
		}

		return false;
	}

	private static boolean interfaceFollowsMXBeanConvention(Class<?> interfaze) {
		if (interfaze.getSimpleName().endsWith("MXBean") || interfaze.isAnnotationPresent(MXBean.class)) {
			return true;
		}

		Class<?>[] subInterfazes = interfaze.getInterfaces();
		for (Class<?> subInterfaze : subInterfazes) {
			if (interfaceFollowsMXBeanConvention(subInterfaze)) {
				return true;
			}
		}

		return false;
	}

	private static boolean isJmxMBean(Class<?> clazz) {
		return ConcurrentJmxMBean.class.isAssignableFrom(clazz) || EventloopJmxMBean.class.isAssignableFrom(clazz);
	}

	private static boolean isDynamicMBean(Class<?> clazz) {
		return DynamicMBean.class.isAssignableFrom(clazz);
	}

	private static boolean isMBean(Class<?> clazz) {
		return isJmxMBean(clazz) || isStandardMBean(clazz) || isMXBean(clazz) || isDynamicMBean(clazz);
	}

	// region jmx
	@Override
	public int getRegisteredSingletons() {
		return registeredSingletons;
	}

	@Override
	public int getRegisteredPools() {
		return registeredPools;
	}

	@Override
	public int getTotallyRegisteredMBeans() {
		return totallyRegisteredMBeans;
	}

	@Override
	public String getRefreshPeriod() {
		return formatDuration(((JmxMBeans) mbeanFactory).getSpecifiedRefreshPeriod());
	}

	@Override
	public void setRefreshPeriod(String refreshPeriod) {
		((JmxMBeans) mbeanFactory).setRefreshPeriod(parseDuration(refreshPeriod));
	}

	@Override
	public int getMaxRefreshesPerOneCycle() {
		return ((JmxMBeans) mbeanFactory).getMaxJmxRefreshesPerOneCycle();
	}

	@Override
	public void setMaxRefreshesPerOneCycle(int maxRefreshesPerOneCycle) {
		((JmxMBeans) mbeanFactory).setMaxJmxRefreshesPerOneCycle(maxRefreshesPerOneCycle);
	}

	@Override
	public double[] getEffectiveRefreshPeriods() {
		List<Integer> effectivePeriods =
				new ArrayList<>(((JmxMBeans) mbeanFactory).getEffectiveRefreshPeriods().values());
		double[] effectivePeriodsSeconds = new double[effectivePeriods.size()];
		for (int i = 0; i < effectivePeriods.size(); i++) {
			int periodMillis = effectivePeriods.get(i);
			effectivePeriodsSeconds[i] = periodMillis / (double) 1000;

		}
		return effectivePeriodsSeconds;
	}

	@Override
	public int[] getRefreshableStatsCount() {
		List<Integer> counts = new ArrayList<>(((JmxMBeans) mbeanFactory).getRefreshableStatsCounts().values());
		int[] refreshableStatsCountsArr = new int[counts.size()];
		for (int i = 0; i < counts.size(); i++) {
			Integer count = counts.get(i);
			refreshableStatsCountsArr[i] = count;
		}
		return refreshableStatsCountsArr;
	}
	// endregion
}
