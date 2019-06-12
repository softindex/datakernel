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

package io.global.launchers;

import io.datakernel.async.EventloopTaskScheduler;
import io.datakernel.config.ConfigModule;
import io.datakernel.di.annotation.Inject;
import io.datakernel.di.annotation.Named;
import io.datakernel.di.module.Module;
import io.datakernel.http.AsyncHttpServer;
import io.datakernel.jmx.JmxModule;
import io.datakernel.launcher.Launcher;
import io.datakernel.service.ServiceGraphModule;

import static io.datakernel.config.Config.ofClassPathProperties;
import static io.datakernel.config.Config.ofProperties;
import static io.datakernel.di.module.Modules.combine;

public class GlobalNodesLauncher extends Launcher {
	public static final String PROPERTIES_FILE = "global-nodes.properties";

	@Inject
	AsyncHttpServer server;

	@Inject
	@Named("FS push")
	EventloopTaskScheduler fsPushScheduler;

	@Inject
	@Named("FS catch up")
	EventloopTaskScheduler fsCatchUpScheduler;

	@Inject
	@Named("KV push")
	EventloopTaskScheduler kvPushScheduler;

	@Inject
	@Named("KV catch up")
	EventloopTaskScheduler kvCatchUpScheduler;

	@Override
	protected final Module getModule() {
		return combine(
				ServiceGraphModule.defaultInstance(),
				JmxModule.create(),
				ConfigModule.create(() ->
						ofClassPathProperties(PROPERTIES_FILE)
								.override(ofProperties(System.getProperties()).getChild("config")))
						.printEffectiveConfig(),
				new GlobalNodesModule()
		);
	}

	@Override
	protected void run() throws Exception {
		awaitShutdown();
	}

	public static void main(String[] args) throws Exception {
		new GlobalNodesLauncher().launch(args);
	}
}
