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
import io.datakernel.crdt.util.CrdtDataSerializer;
import io.datakernel.crdt.util.TimestampContainer;
import io.datakernel.di.annotation.Provides;
import io.datakernel.di.module.AbstractModule;
import io.datakernel.di.module.Module;
import io.datakernel.eventloop.Eventloop;
import io.datakernel.launcher.Launcher;
import io.datakernel.remotefs.FsClient;
import io.datakernel.remotefs.LocalFsClient;

import java.util.concurrent.Executor;

import static io.datakernel.codec.StructuredCodecs.*;
import static io.datakernel.config.converter.ConfigConverters.ofExecutor;
import static io.datakernel.config.converter.ConfigConverters.ofPath;
import static io.datakernel.serializer.BinarySerializers.INT_SERIALIZER;
import static io.datakernel.serializer.BinarySerializers.UTF8_SERIALIZER;
import static java.util.concurrent.Executors.newSingleThreadExecutor;

public final class CrdtNodeExample extends CrdtNodeLauncher<String, TimestampContainer<Integer>> {
	@Override
	protected CrdtNodeLogicModule<String, TimestampContainer<Integer>> getBusinessLogicModule() {
		return new CrdtNodeLogicModule<String, TimestampContainer<Integer>>() {
			@Provides
			CrdtDescriptor<String, TimestampContainer<Integer>> descriptor() {
				return new CrdtDescriptor<>(
						TimestampContainer.createCrdtFunction(Integer::max),
						new CrdtDataSerializer<>(UTF8_SERIALIZER,
								TimestampContainer.createSerializer(INT_SERIALIZER)),
						STRING_CODEC,
						tuple(TimestampContainer::new,
								TimestampContainer::getTimestamp, LONG_CODEC,
								TimestampContainer::getState, INT_CODEC));
			}

			@Provides
			Executor provideExecutor(Config config) {
				return config.get(ofExecutor(), "crdt.local.executor", newSingleThreadExecutor());
			}

			@Provides
			FsClient fsClient(Eventloop eventloop, Executor executor, Config config) {
				return LocalFsClient.create(eventloop, executor, config.get(ofPath(), "crdt.local.path"));
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
						.overrideWith(Config.ofSystemProperties("config"));
			}
		};
	}

	public static void main(String[] args) throws Exception {
		Launcher launcher = new CrdtNodeExample();
		launcher.launch(args);
	}
}
