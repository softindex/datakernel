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
import io.datakernel.di.module.Module;
import io.datakernel.eventloop.PrimaryServer;
import io.datakernel.launcher.Launcher;
import io.datakernel.service.ServiceGraphModule;

import static io.datakernel.di.module.Modules.combine;

// [START EXAMPLE]
public class HttpHelloWorldLauncher extends Launcher {
	@Inject
	PrimaryServer server;

	@Override
	protected Module getModule() {
		return combine(
				ServiceGraphModule.defaultInstance(),
				ConfigModule.create(Config.create()
						.with("port", "8080")
						.with("workers", "4")
						.with("message", "Hello from config")),
				new HttpHelloWorldModule()
		);
	}

	@Override
	protected void run() throws Exception {
		awaitShutdown();
	}

	public static void main(String[] args) throws Exception {
		HttpHelloWorldLauncher launcher = new HttpHelloWorldLauncher();
		launcher.launch(args);
	}
}
// [END EXAMPLE]
