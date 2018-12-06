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

package io.datakernel.examples;

import com.google.inject.AbstractModule;
import com.google.inject.Module;
import com.google.inject.Provides;
import com.google.inject.Singleton;
import io.datakernel.config.Config;
import io.datakernel.config.ConfigModule;
import io.datakernel.eventloop.Eventloop;
import io.datakernel.launcher.Launcher;
import io.datakernel.launchers.remotefs.RemoteFsServerLauncher;

import java.util.Collection;

import static io.datakernel.eventloop.FatalErrorHandlers.rethrowOnAnyError;
import static io.datakernel.launchers.remotefs.RemoteFsServerLauncher.EAGER_SINGLETONS_MODE;
import static java.lang.Boolean.parseBoolean;
import static java.util.Arrays.asList;

/**
 * This example demonstrates configuring and launching RemoteFsServer.
 */
public class ServerSetupExample {
	public static void main(String[] args) throws Exception {
		Launcher launcher = new RemoteFsServerLauncher() {
			@Override
			protected Collection<Module> getOverrideModules() {
				return asList(
						ConfigModule.create(Config.create()
								.with("remotefs.path", "src/main/resources/server_storage")
								.with("remotefs.listenAddresses", "6732")
						),
						new AbstractModule() {
							@Provides
							@Singleton
							Eventloop eventloop() {
								return Eventloop.create()
										.withFatalErrorHandler(rethrowOnAnyError())
										.withCurrentThread();
							}
						}
				);
			}
		};
		launcher.launch(parseBoolean(System.getProperty(EAGER_SINGLETONS_MODE)), args);
	}
}
