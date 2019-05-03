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

package io.datakernel.launchers.remotefs;

import com.google.inject.Module;
import io.datakernel.config.Config;
import io.datakernel.config.ConfigModule;
import io.datakernel.test.rules.ByteBufRule;
import io.datakernel.test.rules.EventloopRule;
import org.junit.ClassRule;
import org.junit.Ignore;
import org.junit.Test;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Collection;
import java.util.Random;

import static io.datakernel.config.ConfigConverters.ofInetSocketAddress;
import static io.datakernel.test.TestUtils.getFreePort;
import static java.util.Collections.singleton;

@Ignore
public final class RemoteFsClusterLauncherTest {
	private static final int serverNumber = getFreePort();

	@ClassRule
	public static final EventloopRule eventloopRule = new EventloopRule();

	@ClassRule
	public static final ByteBufRule byteBufRule = new ByteBufRule();

	@Test
	public void testInjector() {
		new RemoteFsClusterLauncher() {
		}.testInjector();
	}

	@Test
	@Ignore("startup point for the testing launcher override")
	public void launchServer() throws Exception {
		new RemoteFsServerLauncher() {
			@Override
			protected Collection<Module> getOverrideModules() {
				return singleton(ConfigModule.create(Config.create()
					.with("remotefs.path", Config.ofValue("storages/server_" + serverNumber))
					.with("remotefs.listenAddresses", Config.ofValue(ofInetSocketAddress(), new InetSocketAddress(serverNumber)))));
			}
		}.launch(false, new String[0]);
	}

	@Test
	@Ignore("manual startup point for the testing launcher override")
	public void launchCluster() throws Exception {
		long start = System.nanoTime();
		createFiles(Paths.get("storages/local"), 1000, 10 * 1024, 100 * 1024);
		System.out.println("Created local files in " + ((System.nanoTime() - start) / 1e6) + " ms");

		new RemoteFsClusterLauncher() {
			@Override
			protected Collection<Module> getOverrideModules() {
				Config config = Config.create()
					.with("local.listenAddresses", Config.ofValue(ofInetSocketAddress(), new InetSocketAddress(8000)))
					.with("local.path", Config.ofValue("storages/local"))
					.with("cluster.replicationCount", Config.ofValue("3"))
					.with("scheduler.repartition.disabled", "true");
				for (int i = 0; i < 10; i++) {
					config = config.with("cluster.partitions.server_" + i, "localhost:" + (5400 + i));
				}
				return singleton(ConfigModule.create(config));
			}
		}.launch(false, new String[0]);
	}

	private static void createFiles(Path path, int n, int minSize, int maxSize) throws IOException {
		Files.createDirectories(path);
		int delta = maxSize - minSize;
		Random rng = new Random(7L);
		for (int i = 0; i < n; i++) {
			byte[] data = new byte[minSize + (delta <= 0 ? 0 : rng.nextInt(delta))];
			rng.nextBytes(data);
			Files.write(path.resolve("file_" + i + ".txt"), data);
		}
	}
}
