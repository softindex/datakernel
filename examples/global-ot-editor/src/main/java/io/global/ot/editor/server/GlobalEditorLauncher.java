/*
 * Copyright (C) 2015-2019 SoftIndex LLC.
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

package io.global.ot.editor.server;

import com.google.inject.Inject;
import com.google.inject.Module;
import com.google.inject.name.Named;
import io.datakernel.config.Config;
import io.datakernel.config.ConfigModule;
import io.datakernel.http.AsyncHttpServer;
import io.datakernel.launcher.Launcher;
import io.datakernel.service.ServiceGraphModule;
import io.global.common.ExampleCommonModule;
import io.global.launchers.GlobalNodesModule;

import java.util.Collection;

import static com.google.inject.util.Modules.override;
import static io.datakernel.config.Config.ofProperties;
import static java.lang.Boolean.parseBoolean;
import static java.util.Arrays.asList;

public final class GlobalEditorLauncher extends Launcher {
	public static final String EAGER_SINGLETONS_MODE = "eagerSingletonsMode";
	public static final String PROPERTIES_FILE = "server.properties";
	public static final String CREDENTIALS_FILE = "credentials.properties";
	public static final String DEFAULT_LISTEN_ADDRESSES = "*:8080";
	public static final String DEFAULT_SERVER_ID = "Editor Node";

	@Inject
	@Named("Editor")
	AsyncHttpServer server;

	@Override
	protected Collection<Module> getModules() {
		return asList(
				ServiceGraphModule.defaultInstance(),
				ConfigModule.create(() ->
						Config.create()
								.with("http.listenAddresses", DEFAULT_LISTEN_ADDRESSES)
								.with("ot.serverId", DEFAULT_SERVER_ID)
								.override(Config.ofProperties(PROPERTIES_FILE, true)
										.combine(Config.ofProperties(CREDENTIALS_FILE, true)))
								.override(ofProperties(System.getProperties()).getChild("config")))
						.printEffectiveConfig(),
				override(new GlobalEditorModule(), new GlobalNodesModule())
						.with(new ExampleCommonModule()));
	}

	@Override
	protected void run() throws Exception {
		awaitShutdown();
	}

	public static void main(String[] args) throws Exception {
		new GlobalEditorLauncher().launch(parseBoolean(System.getProperty(EAGER_SINGLETONS_MODE)), args);
	}
}
