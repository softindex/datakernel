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
import io.datakernel.crdt.CrdtData.CrdtDataSerializer;
import io.datakernel.di.annotation.Provides;
import io.datakernel.di.module.AbstractModule;
import io.datakernel.di.module.Module;
import io.datakernel.eventloop.Eventloop;
import io.datakernel.launcher.Launcher;
import io.datakernel.remotefs.FsClient;
import io.datakernel.remotefs.LocalFsClient;

import static io.datakernel.codec.StructuredCodecs.INT_CODEC;
import static io.datakernel.codec.StructuredCodecs.STRING_CODEC;
import static io.datakernel.config.ConfigConverters.ofPath;
import static io.datakernel.serializer.BinarySerializers.INT_SERIALIZER;
import static io.datakernel.serializer.BinarySerializers.UTF8_SERIALIZER;

public final class CrdtNodeExample extends CrdtNodeLauncher<String, Integer> {
	@Override
	protected CrdtNodeLogicModule<String, Integer> getBusinessLogicModule() {
		return new CrdtNodeLogicModule<String, Integer>() {
			@Provides
			CrdtDescriptor<String, Integer> descriptor() {
				return new CrdtDescriptor<>(
						Integer::max,
						new CrdtDataSerializer<>(UTF8_SERIALIZER, INT_SERIALIZER),
						STRING_CODEC,
						INT_CODEC);
			}

			@Provides
			FsClient fsClient(Eventloop eventloop, Config config) {
				return LocalFsClient.create(eventloop, config.get(ofPath(), "crdt.local.path"));
			}
		};
	}

	@Override
	protected Module getOverrideModule() {
		return new AbstractModule() {
			@Provides
			Config config() {
				return Config.create()
						.with("crdt.http.listenAddresses", "localhost:8080")
						.with("crdt.server.listenAddresses", "localhost:9090")
						.with("crdt.local.path", "/tmp/TESTS/crdt")
						.with("crdt.cluster.localPartitionId", "local")
						.with("crdt.cluster.partitions.noop", "localhost:9091")
						.overrideWith(Config.ofClassPathProperties(PROPERTIES_FILE, true))
						.overrideWith(Config.ofProperties(System.getProperties()).getChild("config"));
			}
		};
	}

	public static void main(String[] args) throws Exception {
		Launcher launcher = new CrdtNodeExample();
		launcher.launch(args);
	}
}
