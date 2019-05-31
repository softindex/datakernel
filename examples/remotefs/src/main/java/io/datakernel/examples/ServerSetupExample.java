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

import io.datakernel.config.Config;
import io.datakernel.config.ConfigModule;
import io.datakernel.di.Inject;
import io.datakernel.di.module.AbstractModule;
import io.datakernel.di.module.Module;
import io.datakernel.di.module.Provides;
import io.datakernel.eventloop.Eventloop;
import io.datakernel.exception.UncheckedException;
import io.datakernel.launcher.Launcher;
import io.datakernel.launchers.remotefs.RemoteFsServerLauncher;

import java.io.IOException;
import java.nio.file.Files;

import static io.datakernel.di.module.Modules.combine;

/**
 * This example demonstrates configuring and launching RemoteFsServer.
 */
public class ServerSetupExample extends RemoteFsServerLauncher {
	@Override
	protected Module getOverrideModule() {
		try {
			return combine(
					ConfigModule.create(Config.create()
							.with("remotefs.path", Files.createTempDirectory("server_storage")
									.toString())
							.with("remotefs.listenAddresses", "6732")
					),
					new AbstractModule() {
						@Provides
						Eventloop eventloop() {
							return Eventloop.create();
						}
					}
			);
		} catch (IOException e) {
			throw new UncheckedException(e);
		}
	}

	@Override
	protected void run() throws Exception {
		awaitShutdown();
	}

	public static void main(String[] args) throws Exception {
		Launcher launcher = new ServerSetupExample();
		launcher.launch(args);
	}
}
