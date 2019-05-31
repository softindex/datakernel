/*
 * Copyright (C) 2015-2018 SoftIndex LLC.
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

package io.datakernel.launchers.crdt;

import io.datakernel.config.Config;
import io.datakernel.config.ConfigModule;
import io.datakernel.crdt.CrdtServer;
import io.datakernel.di.Inject;
import io.datakernel.di.module.Module;
import io.datakernel.jmx.JmxModule;
import io.datakernel.launcher.Launcher;
import io.datakernel.launchers.crdt.CrdtNodeLogicModule.Cluster;
import io.datakernel.service.ServiceGraphModule;
import io.datakernel.trigger.TriggersModule;

import static io.datakernel.config.Config.ofClassPathProperties;
import static io.datakernel.config.Config.ofProperties;
import static io.datakernel.di.module.Modules.combine;
import static io.datakernel.di.module.Modules.override;

public abstract class CrdtNodeLauncher<K extends Comparable<K>, S> extends Launcher {
	public static final String PROPERTIES_FILE = "crdt-node.properties";

	@Inject
	@Cluster
	CrdtServer<K, S> clusterServer;

	@Inject
	CrdtServer<K, S> crdtServer;

	private CrdtNodeLogicModule<K, S> logicModule = getLogicModule();

	@Override
	protected Module getModule() {
		return combine(
				override(getBaseModule(), getOverrideModule()),
				getBusinessLogicModule());
	}

	protected Module getOverrideModule() {
		return Module.empty();
	}

	protected abstract CrdtNodeLogicModule<K, S> getLogicModule();

	protected abstract Module getBusinessLogicModule();

	private Module getBaseModule() {
		return combine(
				ServiceGraphModule.defaultInstance(),
				JmxModule.create(),
				TriggersModule.create(),
				ConfigModule.create(() ->
						Config.create()
								.override(ofClassPathProperties(PROPERTIES_FILE, true))
								.override(ofProperties(System.getProperties()).getChild("config")))
						.printEffectiveConfig(),
				logicModule
		);
	}

	@Override
	protected void run() throws Exception {
		awaitShutdown();
	}
}
