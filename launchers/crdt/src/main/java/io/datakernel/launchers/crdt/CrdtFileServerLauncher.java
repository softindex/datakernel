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
import io.datakernel.crdt.remote.CrdtRemoteServer;
import io.datakernel.crdt.local.CrdtClientFs;
import io.datakernel.di.annotation.Inject;
import io.datakernel.di.annotation.Provides;
import io.datakernel.di.core.Key;
import io.datakernel.di.module.AbstractModule;
import io.datakernel.di.module.Module;
import io.datakernel.eventloop.Eventloop;
import io.datakernel.jmx.JmxModule;
import io.datakernel.launcher.Launcher;
import io.datakernel.launcher.OnStart;
import io.datakernel.remotefs.LocalFsClient;
import io.datakernel.service.ServiceGraphModule;
import io.datakernel.trigger.TriggersModule;

import java.util.concurrent.CompletionStage;
import java.util.concurrent.ExecutorService;

import static io.datakernel.config.Config.ofClassPathProperties;
import static io.datakernel.config.Config.ofProperties;
import static io.datakernel.config.ConfigConverters.ofExecutor;
import static io.datakernel.config.ConfigConverters.ofPath;
import static io.datakernel.di.module.Modules.combine;
import static io.datakernel.launchers.initializers.Initializers.ofAbstractServer;

public abstract class CrdtFileServerLauncher<K extends Comparable<K>, S> extends Launcher {
	public static final String PROPERTIES_FILE = "crdt-file-server.properties";

	@Inject
	CrdtRemoteServer<K, S> crdtServer;

	@Provides
	Eventloop eventloop() {
		return Eventloop.create();
	}

	@Provides
	ExecutorService executor(Config config) {
		return config.get(ofExecutor(), "executor");
	}

	@Provides
	LocalFsClient localFsClient(Eventloop eventloop, ExecutorService executor, Config config) {
		return LocalFsClient.create(eventloop, config.get(ofPath(), "crdt.localPath"));
	}

	@Provides
	Config config() {
		return Config.create()
				.overrideWith(ofClassPathProperties(PROPERTIES_FILE, true))
				.overrideWith(ofProperties(System.getProperties()).getChild("config"));
	}

	@Override
	protected Module getModule() {
		return combine(
				ServiceGraphModule.create(),
				JmxModule.create(),
				TriggersModule.create(),
				ConfigModule.create()
						.printEffectiveConfig()
						.rebindImport(new Key<CompletionStage<Void>>() {}, new Key<CompletionStage<Void>>(OnStart.class) {}),
				getBusinessLogicModule());
	}

	protected abstract CrdtFileServerLogicModule<K, S> getBusinessLogicModule();

	public abstract static class CrdtFileServerLogicModule<K extends Comparable<K>, S> extends AbstractModule {
		@Provides
		CrdtRemoteServer<K, S> crdtServer(Eventloop eventloop, CrdtClientFs<K, S> crdtClient, CrdtDescriptor<K, S> descriptor, Config config) {
			return CrdtRemoteServer.create(eventloop, crdtClient, descriptor.getSerializer())
					.initialize(ofAbstractServer(config.getChild("crdt.server")));
		}

		@Provides
        CrdtClientFs<K, S> fsCrdtClient(Eventloop eventloop, LocalFsClient localFsClient, CrdtDescriptor<K, S> descriptor, Config config) {
			return CrdtClientFs.create(eventloop, localFsClient, descriptor.getSerializer(), descriptor.getCrdtOperator())
					.initialize(Initializers.ofFsCrdtClient(config.getChild("crdt.files")));
		}
	}

	@Override
	protected void run() throws Exception {
		awaitShutdown();
	}
}
