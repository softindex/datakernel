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
import io.datakernel.crdt.CrdtDataSerializer;
import io.datakernel.crdt.TimestampContainer;
import io.datakernel.di.module.AbstractModule;
import io.datakernel.di.module.Module;
import io.datakernel.di.module.Provides;
import io.datakernel.eventloop.Eventloop;
import io.datakernel.launcher.Launcher;
import io.datakernel.remotefs.FsClient;
import io.datakernel.remotefs.LocalFsClient;

import java.util.Collection;

import static io.datakernel.codec.StructuredCodecs.*;
import static io.datakernel.config.Config.ofProperties;
import static io.datakernel.config.ConfigConverters.ofPath;
import static io.datakernel.serializer.util.BinarySerializers.INT_SERIALIZER;
import static io.datakernel.serializer.util.BinarySerializers.UTF8_SERIALIZER;
import static java.util.Collections.singletonList;

public final class CrdtNodeExample {
	public static final String EAGER_SINGLETONS_MODE = "eagerSingletonsMode";

	static class BusinessLogicModule extends AbstractModule {
		@Provides
		CrdtDescriptor<String, TimestampContainer<Integer>> provideDescriptor() {
			return new CrdtDescriptor<>(TimestampContainer.createCrdtFunction(Integer::max),
					new CrdtDataSerializer<>(UTF8_SERIALIZER, TimestampContainer.createSerializer(INT_SERIALIZER)), STRING_CODEC,
					tuple(TimestampContainer::new, TimestampContainer::getTimestamp, LONG_CODEC, TimestampContainer::getState, INT_CODEC));
		}

		@Provides
		FsClient provideFsClient(Eventloop eventloop, Config config) {
			return LocalFsClient.create(eventloop, config.get(ofPath(), "crdt.local.path"));
		}
	}

	public static void main(String[] args) throws Exception {
		Launcher launcher = new CrdtNodeLauncher<String, TimestampContainer<Integer>>() {

			@Override
			protected CrdtNodeLogicModule<String, TimestampContainer<Integer>> getLogicModule() {
				return new CrdtNodeLogicModule<String, TimestampContainer<Integer>>() {};
			}

			@Override
			protected Collection<Module> getOverrideModules() {
				return singletonList(
						ConfigModule.create(() ->
								Config.create()
										.with("crdt.http.listenAddresses", "localhost:8080")
										.with("crdt.server.listenAddresses", "localhost:9090")
										.with("crdt.local.path", "/tmp/TESTS/crdt")
										.with("crdt.cluster.localPartitionId", "local")
										.with("crdt.cluster.partitions.noop", "localhost:9091")
										.override(ofProperties(PROPERTIES_FILE, true))
										.override(ofProperties(System.getProperties()).getChild("config")))
								.printEffectiveConfig());
			}

			@Override
			protected Collection<Module> getBusinessLogicModules() {
				return singletonList(new BusinessLogicModule());
			}
		};
		launcher.launch(args);
	}
}
