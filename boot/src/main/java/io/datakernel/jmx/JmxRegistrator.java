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

import com.google.inject.Injector;
import com.google.inject.Key;
import com.google.inject.name.Names;
import io.datakernel.async.CallbackRegistry;
import io.datakernel.bytebuf.ByteBufPool;
import io.datakernel.worker.WorkerPools;

import javax.management.DynamicMBean;
import java.lang.reflect.Type;
import java.util.*;

public final class JmxRegistrator {
	private final Injector injector;
	private final Set<Key<?>> singletonKeys;
	private final Set<Key<?>> workerKeys;
	private final JmxRegistry jmxRegistry;
	private final DynamicMBeanFactory mBeanFactory;
	private final Map<Key<?>, MBeanSettings> keyToSettings;
	private final Map<Type, MBeanSettings> typeToSettings;
	private final Map<Type, String> globalMBeans;

	private JmxRegistrator(Injector injector,
	                       Set<Key<?>> singletonKeys, Set<Key<?>> workerKeys,
	                       JmxRegistry jmxRegistry, DynamicMBeanFactory mbeanFactory,
	                       Map<Key<?>, MBeanSettings> keyToSettings,
	                       Map<Type, MBeanSettings> typeToSettings,
	                       Map<Type, String> globalMBeans) {
		this.injector = injector;
		this.singletonKeys = singletonKeys;
		this.workerKeys = workerKeys;
		this.jmxRegistry = jmxRegistry;
		this.mBeanFactory = mbeanFactory;
		this.keyToSettings = keyToSettings;
		this.typeToSettings = typeToSettings;
		this.globalMBeans = globalMBeans;
	}

	public static JmxRegistrator create(Injector injector,
	                                    Set<Key<?>> singletonKeys, Set<Key<?>> workerKeys,
	                                    JmxRegistry jmxRegistry, DynamicMBeanFactory mbeanFactory,
	                                    Map<Key<?>, MBeanSettings> keyToSettings,
	                                    Map<Type, MBeanSettings> typeToSettings,
	                                    Map<Type, String> globalMBeans) {
		return new JmxRegistrator(injector, singletonKeys, workerKeys, jmxRegistry, mbeanFactory, keyToSettings,
				typeToSettings, globalMBeans);
	}

	public void registerJmxMBeans() {
		// register ByteBufPool
		Key<?> byteBufPoolKey = Key.get(ByteBufPool.ByteBufPoolStats.class);
		jmxRegistry.registerSingleton(byteBufPoolKey, ByteBufPool.getStats(), MBeanSettings.defaultSettings());

		// register CallbackRegistry
		Key<?> callbackRegistryKey = Key.get(CallbackRegistry.class);
		jmxRegistry.registerSingleton(
				callbackRegistryKey, new CallbackRegistry.CallbackRegistryStats(), MBeanSettings.defaultSettings());

		Map<Type, List<Object>> globalMBeanObjects = new HashMap<>();

		// register singletons
		for (Key<?> key : singletonKeys) {
			Object instance = injector.getInstance(key);
			jmxRegistry.registerSingleton(key, instance, ensureSettingsFor(key));

			Type type = key.getTypeLiteral().getType();
			if (globalMBeans.containsKey(type)) {
				globalMBeanObjects.computeIfAbsent(type, type1 -> new ArrayList<>()).add(instance);
			}
		}

		// register workers
		if (!workerKeys.isEmpty()) {
			WorkerPools workerPools = injector.getInstance(WorkerPools.class);
			for (Key<?> key : workerKeys) {
				List<?> objects = workerPools.getWorkerPoolObjects(key).getObjects();
				jmxRegistry.registerWorkers(key, objects, ensureSettingsFor(key));

				Type type = key.getTypeLiteral().getType();
				if (globalMBeans.containsKey(type)) {
					for (Object workerObject : objects) {
						globalMBeanObjects.computeIfAbsent(type, type1 -> new ArrayList<>()).add(workerObject);
					}
				}
			}
		}

		for (Type type : globalMBeanObjects.keySet()) {
			List<Object> objects = globalMBeanObjects.get(type);
			String globalMBeanName = globalMBeans.get(type);
			Key<?> key = Key.get(type, Names.named(globalMBeanName));
			DynamicMBean globalMBean =
					mBeanFactory.createFor(objects, ensureSettingsFor(key), false);
			jmxRegistry.registerSingleton(key, globalMBean, null);
		}
	}

	private MBeanSettings ensureSettingsFor(Key<?> key) {
		if (keyToSettings.containsKey(key)) {
			return keyToSettings.get(key);
		}
		if (typeToSettings.containsKey(key.getTypeLiteral().getType())) {
			return typeToSettings.get(key.getTypeLiteral().getType());
		}
		return MBeanSettings.defaultSettings();
	}
}
