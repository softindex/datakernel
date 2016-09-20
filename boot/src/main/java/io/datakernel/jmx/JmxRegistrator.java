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
import io.datakernel.bytebuf.ByteBufPool;
import io.datakernel.worker.WorkerPools;

import java.util.List;
import java.util.Set;

public final class JmxRegistrator {
	private final Injector injector;
	private final Set<Key<?>> singletonKeys;
	private final Set<Key<?>> workerKeys;
	private final JmxRegistry jmxRegistry;

	private JmxRegistrator(Injector injector,
	                       Set<Key<?>> singletonKeys, Set<Key<?>> workerKeys,
	                       JmxRegistry jmxRegistry) {
		this.injector = injector;
		this.singletonKeys = singletonKeys;
		this.workerKeys = workerKeys;
		this.jmxRegistry = jmxRegistry;
	}

	public static JmxRegistrator create(Injector injector,
	                                    Set<Key<?>> singletonKeys, Set<Key<?>> workerKeys,
	                                    JmxRegistry jmxRegistry) {return new JmxRegistrator(injector, singletonKeys, workerKeys, jmxRegistry);}

	public void registerJmxMBeans() {
		// register ByteBufPool
		Key<?> byteBufPoolKey = Key.get(ByteBufPool.ByteBufPoolStats.class);
		jmxRegistry.registerSingleton(byteBufPoolKey, ByteBufPool.getStats());

		// register singletons
		for (Key<?> key : singletonKeys) {
			Object instance = injector.getInstance(key);
			jmxRegistry.registerSingleton(key, instance);
		}

		// register workers
		if (!workerKeys.isEmpty()) {
			WorkerPools workerPools = injector.getInstance(WorkerPools.class);
			for (Key<?> key : workerKeys) {
				List<?> objects = workerPools.getWorkerPoolObjects(key).getObjects();
				jmxRegistry.registerWorkers(key, objects);
			}
		}

	}
}
