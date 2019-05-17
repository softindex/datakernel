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

import com.google.inject.Inject;
import com.google.inject.name.Named;
import io.datakernel.async.EventloopTaskScheduler;
import io.datakernel.http.AsyncHttpServer;
import io.datakernel.launcher.Launcher;

import java.util.Collection;

import static com.google.inject.util.Modules.override;
import static io.datakernel.config.Config.ofProperties;
import static java.util.Arrays.asList;
import static java.util.Collections.emptyList;
import static java.util.Collections.singletonList;

public class GlobalNodesLauncher extends Launcher {
	public static final String EAGER_SINGLETONS_MODE = "eagerSingletonsMode";
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
	@Named("DB push")
	EventloopTaskScheduler kvPushScheduler;

	@Inject
	@Named("DB catch up")
	EventloopTaskScheduler kvCatchUpScheduler;

	@Override
	protected final Collection<com.google.inject.Module> getModules() {
		return singletonList(override(getBaseModules()).with(getOverrideModules()));
	}

	private Collection<com.google.inject.Module> getBaseModules() {
		return asList(
				ServiceGraphModule.defaultInstance(),
				JmxModule.create(),
				ConfigModule.create(() ->
						ofProperties(PROPERTIES_FILE)
								.override(ofProperties(System.getProperties()).getChild("config")))
						.printEffectiveConfig(),
				new GlobalNodesModule()
		);
	}

	/**
	 * Override this method to override base modules supplied in launcher.
	 */
	protected Collection<com.google.inject.Module> getOverrideModules() {
		return emptyList();
	}

	@Override
	protected void run() throws Exception {
		awaitShutdown();
	}

	public static void main(String[] args) throws Exception {
		new GlobalNodesLauncher().launch(args);
	}

}
