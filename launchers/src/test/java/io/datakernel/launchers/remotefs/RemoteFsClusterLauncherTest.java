package io.datakernel.launchers.remotefs;

import com.google.inject.Module;
import io.datakernel.config.Config;
import io.datakernel.config.ConfigModule;
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
import static java.util.Collections.singleton;

public class RemoteFsClusterLauncherTest {
	@Test
	public void testInjector() {
		new RemoteFsClusterLauncher() {
		}.testInjector();
	}

	static int serverNumber = 5400;

	@Test
	@Ignore
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
	@Ignore
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