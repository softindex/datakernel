package io.global.globalfs;

import io.datakernel.bytebuf.ByteBuf;
import io.datakernel.bytebuf.ByteBufQueue;
import io.datakernel.eventloop.Eventloop;
import io.datakernel.remotefs.FsClient;
import io.datakernel.remotefs.LocalFsClient;
import io.datakernel.serial.SerialSupplier;
import io.global.common.KeyPair;
import io.global.common.RawServerId;
import io.global.common.api.DiscoveryService;
import io.global.globalfs.api.GlobalFsName;
import io.global.globalfs.api.GlobalFsNode;
import io.global.globalfs.client.GlobalFsAdapter;
import io.global.globalfs.server.GlobalFsLocalNode;
import io.global.globalfs.server.LocalDiscoveryService;
import io.global.globalfs.server.RawNodeFactory;
import io.global.globalfs.server.RemoteFsFileSystem;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.time.Duration;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static io.datakernel.eventloop.FatalErrorHandlers.rethrowOnAnyError;
import static io.datakernel.test.TestUtils.assertComplete;
import static java.nio.charset.StandardCharsets.UTF_8;
import static org.junit.Assert.assertEquals;

public class ServerTest {

	@Rule
	public TemporaryFolder temporaryFolder = new TemporaryFolder();

	@Test
	public void test() throws IOException {
		Eventloop eventloop = Eventloop.create().withCurrentThread().withFatalErrorHandler(rethrowOnAnyError());
		ExecutorService executor = Executors.newSingleThreadExecutor();

		FsClient fs = LocalFsClient.create(eventloop, executor, temporaryFolder.newFolder().toPath());
		DiscoveryService discoveryService = new LocalDiscoveryService();

		RawNodeFactory clientFactory = new RawNodeFactory() {
			int serverIndex = 0;

			@Override
			public GlobalFsNode create(RawServerId serverId) {
				return new GlobalFsLocalNode(serverId, discoveryService, this,
						RemoteFsFileSystem.usingSingleClient(fs.subfolder("server_" + serverIndex++)),
						() -> Duration.ofMinutes(5));
			}
		};

		GlobalFsNode client = clientFactory.create(new RawServerId(new InetSocketAddress(InetAddress.getLocalHost(), 12323)));
		KeyPair alice = KeyPair.generate();
		KeyPair bob = KeyPair.generate();

		FsClient adapted = new GlobalFsAdapter(client.getFileSystem(GlobalFsName.of(alice, "testFs")).getResult(), alice, pos -> pos + 3);
		FsClient other = new GlobalFsAdapter(client.getFileSystem(GlobalFsName.of(bob, "testFs")).getResult(), bob, pos -> pos + 4);

		String content = "hello world, i am here!";

		adapted.upload("test.txt")
				.thenCompose(consumer -> SerialSupplier.of(ByteBuf.wrapForReading(content.getBytes(UTF_8))).streamTo(consumer))
				.thenCompose($ -> adapted.download("test.txt"))
				.thenCompose(supplier -> supplier.toCollector(ByteBufQueue.collector()))
				.whenResult(buf -> {
					System.out.println(buf.asString(UTF_8));
					assertEquals(content, buf.asString(UTF_8));
				})
				.thenCompose($ -> other.download("test.txt"))
				.whenComplete(assertComplete());

		eventloop.run();
	}
}
