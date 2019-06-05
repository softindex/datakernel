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

package io.global.ot.editor;

import io.datakernel.codec.StructuredCodec;
import io.datakernel.config.Config;
import io.datakernel.config.ConfigModule;
import io.datakernel.di.Inject;
import io.datakernel.di.Key;
import io.datakernel.di.Named;
import io.datakernel.di.module.Module;
import io.datakernel.http.AsyncHttpServer;
import io.datakernel.launcher.Launcher;
import io.datakernel.ot.OTSystem;
import io.datakernel.service.ServiceGraphModule;
import io.global.common.ExampleCommonModule;
import io.global.common.ot.OTCommonModule;
import io.global.launchers.GlobalNodesModule;
import io.global.ot.editor.operations.EditorOperation;

import java.util.Objects;
import java.util.function.Function;

import static io.datakernel.config.Config.ofProperties;
import static io.datakernel.di.module.Modules.combine;
import static io.datakernel.di.module.Modules.override;
import static io.global.ot.editor.operations.EditorOTSystem.createOTSystem;
import static io.global.ot.editor.operations.Utils.OPERATION_CODEC;

public final class GlobalEditorDemoApp extends Launcher {
	public static final String PROPERTIES_FILE = "server.properties";
	public static final String CREDENTIALS_FILE = "credentials.properties";
	public static final String DEFAULT_LISTEN_ADDRESSES = "*:8080";
	public static final String DEFAULT_SERVER_ID = "Editor Node";
	public static final String DEFAULT_RESOURCES = "/build";

	@Inject
	@Named("Example")
	AsyncHttpServer server;

	@Override
	protected Module getModule() {
		return combine(
				ServiceGraphModule.defaultInstance(),
				ConfigModule.create(() ->
						Config.create()
								.with("http.listenAddresses", DEFAULT_LISTEN_ADDRESSES)
								.with("node.serverId", DEFAULT_SERVER_ID)
								.with("resources.path", DEFAULT_RESOURCES)
								.override(Config.ofClassPathProperties(PROPERTIES_FILE, true)
										.combine(Config.ofClassPathProperties(CREDENTIALS_FILE, true)))
								.override(ofProperties(System.getProperties()).getChild("config")))
						.printEffectiveConfig(),
				new OTCommonModule<EditorOperation>() {
					@Override
					protected void configure() {
						bind(new Key<StructuredCodec<EditorOperation>>() {}).toInstance(OPERATION_CODEC);
						bind(new Key<Function<EditorOperation, String>>() {}).toInstance(Objects::toString);
						bind(new Key<OTSystem<EditorOperation>>() {}).toInstance(createOTSystem());
					}
				},
				override(new GlobalNodesModule(), new ExampleCommonModule())
		);
	}

	@Override
	protected void run() throws Exception {
		awaitShutdown();
	}

	public static void main(String[] args) throws Exception {
		new GlobalEditorDemoApp().launch(args);
	}
}
