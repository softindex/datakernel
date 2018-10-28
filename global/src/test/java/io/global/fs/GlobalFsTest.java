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

package io.global.fs;

import io.datakernel.async.Promises;
import io.datakernel.bytebuf.ByteBuf;
import io.datakernel.bytebuf.ByteBufQueue;
import io.datakernel.eventloop.Eventloop;
import io.datakernel.exception.ParseException;
import io.datakernel.http.AsyncHttpClient;
import io.datakernel.http.AsyncHttpServer;
import io.datakernel.remotefs.FsClient;
import io.datakernel.remotefs.LocalFsClient;
import io.datakernel.serial.SerialSupplier;
import io.datakernel.stream.processor.ActivePromisesRule;
import io.global.common.KeyPair;
import io.global.common.PrivKey;
import io.global.common.RawServerId;
import io.global.common.RepoID;
import io.global.common.api.AnnounceData;
import io.global.common.api.DiscoveryService;
import io.global.fs.api.*;
import io.global.fs.http.GlobalFsNodeServlet;
import io.global.fs.http.HttpDiscoveryService;
import io.global.fs.http.HttpGlobalFsNode;
import io.global.fs.local.GlobalFsGatewayDriver;
import io.global.fs.local.GlobalFsNodeDriver;
import io.global.fs.local.RuntimeDiscoveryService;
import io.global.fs.transformers.FrameSigner;
import org.junit.*;
import org.junit.rules.TemporaryFolder;
import org.spongycastle.crypto.digests.SHA256Digest;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.nio.file.Paths;
import java.time.Instant;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static io.datakernel.bytebuf.ByteBufStrings.wrapUtf8;
import static io.datakernel.eventloop.FatalErrorHandlers.rethrowOnAnyError;
import static io.datakernel.test.TestUtils.*;
import static io.datakernel.util.CollectionUtils.set;
import static io.global.fs.api.CheckpointPosStrategy.fixed;
import static io.global.fs.api.CheckpointPosStrategy.randRange;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.stream.Collectors.toList;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class GlobalFsTest {
	static {
		enableLogging();
	}

	@Rule
	public TemporaryFolder temporaryFolder = new TemporaryFolder();

	@Rule
	public ActivePromisesRule activePromisesRule = new ActivePromisesRule();

	private Eventloop eventloop;
	private ExecutorService executor;

	private NodeFactory clientFactory;
	private DiscoveryService discoveryService;

	private KeyPair alice, bob;

	@Before
	public void setUp() {
		eventloop = Eventloop.create().withCurrentThread().withFatalErrorHandler(rethrowOnAnyError());
		executor = Executors.newSingleThreadExecutor();
		discoveryService = new RuntimeDiscoveryService();

		alice = KeyPair.generate();
		bob = KeyPair.generate();

		FsClient storage = LocalFsClient.create(eventloop, executor, Paths.get("/tmp/TESTS2/")); //temporaryFolder.newFolder().toPath());
		clientFactory = new NodeFactory() {
			@Override
			public GlobalFsNode create(RawServerId serverId) {
				return GlobalFsNodeDriver.create(serverId, discoveryService, this, storage.subfolder("server_" + serverId.getInetSocketAddress().getPort()))
						.withManagedPubKeys(set(alice.getPubKey(), bob.getPubKey()));
			}
		};
	}

	@After
	public void tearDown() {
		executor.shutdownNow();
	}

	@Test
	public void cutters() {
		GlobalFsNode client = clientFactory.create(new RawServerId(new InetSocketAddress(12345)));

		FsClient adapter = GlobalFsGatewayDriver.createFsAdapter(client, alice, "testFs", randRange(25, 50));

		SerialSupplier.of(
				ByteBuf.wrapForReading("hello, this is a test buffer data #01\n".getBytes(UTF_8)),
				ByteBuf.wrapForReading("hello, this is a test buffer data #02\n".getBytes(UTF_8)),
				ByteBuf.wrapForReading("hello, this is a test buffer data #03\n".getBytes(UTF_8)),
				ByteBuf.wrapForReading("hello, this is a test buffer data #04\n".getBytes(UTF_8)),
				ByteBuf.wrapForReading("hello, this is a test buffer data #05\n".getBytes(UTF_8)),
				ByteBuf.wrapForReading("hello, this is a test buffer data #06\n".getBytes(UTF_8)),
				ByteBuf.wrapForReading("hello, this is a test buffer data #07\n".getBytes(UTF_8)),
				ByteBuf.wrapForReading("hello, this is a test buffer data #08\n".getBytes(UTF_8)),
				ByteBuf.wrapForReading("hello, this is a test buffer data #09\n".getBytes(UTF_8)),
				ByteBuf.wrapForReading("hello, this is a test buffer data #10\n".getBytes(UTF_8)),
				ByteBuf.wrapForReading("hello, this is a test buffer data #11\n".getBytes(UTF_8)),
				ByteBuf.wrapForReading("hello, this is a test buffer data #12\n".getBytes(UTF_8)),
				ByteBuf.wrapForReading("hello, this is a test buffer data #13\n".getBytes(UTF_8)),
				ByteBuf.wrapForReading("hello, this is a test buffer data #14\n".getBytes(UTF_8)),
				ByteBuf.wrapForReading("hello, this is a test buffer data #15\n".getBytes(UTF_8))
		)
				.streamTo(adapter.uploadSerial("test1.txt"))
				.whenComplete(($, e) -> System.out.println("FINISHED UPLOADING"))
				.thenCompose($ -> adapter.download("test1.txt", 10, 380 - 10 - 19))
				.thenCompose(supplier -> supplier.toCollector(ByteBufQueue.collector()))
				.whenComplete(assertComplete(buf ->
						assertEquals("s is a test buffer data #01\n" +
										"hello, this is a test buffer data #02\n" +
										"hello, this is a test buffer data #03\n" +
										"hello, this is a test buffer data #04\n" +
										"hello, this is a test buffer data #05\n" +
										"hello, this is a test buffer data #06\n" +
										"hello, this is a test buffer data #07\n" +
										"hello, this is a test buffer data #08\n" +
										"hello, this is a test buffer data #09\n" +
										"hello, this is a te",
								buf.asString(UTF_8))))
				.thenCompose($ -> adapter.download("test1.txt", 64, 259))
				.thenCompose(supplier -> supplier.toCollector(ByteBufQueue.collector()))
				.whenComplete(assertComplete(buf ->
						assertEquals("er data #02\n" +
										"hello, this is a test buffer data #03\n" +
										"hello, this is a test buffer data #04\n" +
										"hello, this is a test buffer data #05\n" +
										"hello, this is a test buffer data #06\n" +
										"hello, this is a test buffer data #07\n" +
										"hello, this is a test buffer data #08\n" +
										"hello, this is a te",
								buf.asString(UTF_8))))
				.thenCompose($ -> adapter.downloadSerial("test1.txt", 228, 37).toCollector(ByteBufQueue.collector()))
				.whenComplete(assertComplete(buf ->
						assertEquals("hello, this is a test buffer data #07", buf.asString(UTF_8))))
				.thenCompose($ -> adapter.downloadSerial("test1.txt").toCollector(ByteBufQueue.collector()))
				.whenComplete(assertComplete(buf -> assertEquals(570, buf.asString(UTF_8).length())));

		eventloop.run();
	}

	@Test
	public void separate() {
		GlobalFsNode client = clientFactory.create(new RawServerId(new InetSocketAddress(12345)));

		FsClient adapted = GlobalFsGatewayDriver.createFsAdapter(client, alice, "testFs", CheckpointPosStrategy.fixed(3));
		FsClient other = GlobalFsGatewayDriver.createFsAdapter(client, bob, "testFs", CheckpointPosStrategy.fixed(4));

		String content = "hello world, i am here!";

		adapted.upload("test.txt")
				.thenCompose(consumer -> SerialSupplier.of(ByteBuf.wrapForReading(content.getBytes(UTF_8))).streamTo(consumer))
				.thenCompose($ -> adapted.download("test.txt"))
				.thenCompose(supplier -> supplier.toCollector(ByteBufQueue.collector()))
				.whenResult(buf -> assertEquals(content, buf.asString(UTF_8)))
				.thenCompose($ -> other.download("test.txt"))
				.whenComplete(assertFailure());

		eventloop.run();
	}

	@Test
	public void downloadOnlyCheckpoint() {
		GlobalFsNode client = clientFactory.create(new RawServerId(new InetSocketAddress(12345)));
		GlobalPath file = GlobalPath.of(alice, "testFs", "folder/test.txt");

		SerialSupplier.of(wrapUtf8("little test content"))
				.apply(new FrameSigner(file.toLocalPath(), 0, CheckpointPosStrategy.fixed(4), alice.getPrivKey(), new SHA256Digest()))
				.streamTo(client.uploader(file, -1))
				.thenCompose($ -> client.downloader(file, 4, 0).toCollector(toList()))
				.whenComplete(assertComplete(list -> {
					System.out.println(list);
					assertEquals(1, list.size());
					DataFrame frame = list.get(0);
					assertTrue(frame.isCheckpoint());
					assertTrue(frame.getCheckpoint().verify(alice.getPubKey()));
					assertEquals(4, frame.getCheckpoint().getData().getPosition());
				}));

		eventloop.run();
	}

	@Test
	public void uploadWithOffset() {
		GlobalFsNode client = clientFactory.create(new RawServerId(new InetSocketAddress(12345)));
		GlobalPath file = GlobalPath.of(alice, "testFs", "folder/test.txt");

		GlobalFsGateway gateway = GlobalFsGatewayDriver.create(client, alice, fixed(8));

		SerialSupplier.of(wrapUtf8("first line of the content\n"))
				.streamTo(gateway.uploader(file))
				.thenCompose($ ->
						SerialSupplier.of(wrapUtf8("ntent\nsecond line, appended\n"))
								.streamTo(gateway.uploader(file, 20)))
				.thenCompose($ -> gateway.downloader(file).toCollector(ByteBufQueue.collector()))
				.whenResult(buf -> System.out.println(buf.asString(UTF_8)))
				.thenCompose($ -> gateway.getMetadata(file))
				.whenComplete(assertComplete(meta -> assertEquals(48, meta.getSize())));

		eventloop.run();
	}

	@Test
	public void test() throws IOException {
		KeyPair keys = KeyPair.generate();

		GlobalFsNode node = clientFactory.create(new RawServerId(new InetSocketAddress(12345)));
		AsyncHttpServer server = AsyncHttpServer.create(eventloop, new GlobalFsNodeServlet(node))
				.withListenPort(8080);
		server.listen();

		GlobalFsNode client = new HttpGlobalFsNode(new RawServerId(new InetSocketAddress(8080)), AsyncHttpClient.create(eventloop));

		FsClient adapter = GlobalFsGatewayDriver.createFsAdapter(client, keys, "testFs", CheckpointPosStrategy.fixed(5));

		adapter.download("hello/this/is/long/path.txt")
				.whenComplete(assertComplete());

		// String first = "Hello world, this is some bytes ";
		// String second = "to be sent through the GlobalFs HTTP interface";
		//
		// adapter.upload("test.txt")
		// 		.thenCompose(SerialSupplier.of(
		// 				ByteBuf.wrapForReading(first.getBytes(UTF_8)),
		// 				ByteBuf.wrapForReading(second.getBytes(UTF_8)))::streamTo)
		// 		.thenCompose($ -> adapter.downloadSerial("test.txt").toCollector(ByteBufQueue.collector()))
		// 		.whenComplete(assertComplete(res -> assertEquals(first + second, res.asString(UTF_8))))
		// 		.whenComplete(($, e) -> server.close());

		eventloop.run();
	}

	@Test
	@Ignore // requires launched discovery service and two nodes
	public void uploadDownload() throws UnknownHostException {
		KeyPair keys = KeyPair.generate();
		RepoID testFs = RepoID.of(keys, "testFs");

		AsyncHttpClient client = AsyncHttpClient.create(eventloop);
		DiscoveryService discoveryService = HttpDiscoveryService.create(new InetSocketAddress(9001), client);

		RawServerId firstId = new RawServerId(new InetSocketAddress(InetAddress.getLocalHost(), 8001));
		RawServerId secondId = new RawServerId(new InetSocketAddress(InetAddress.getLocalHost(), 8002));

		GlobalFsNode first = new HttpGlobalFsNode(firstId, client);
		GlobalFsNode second = new HttpGlobalFsNode(secondId, client);

		FsClient firstAdapted = GlobalFsGatewayDriver.createFsAdapter(first, keys, "testFs", CheckpointPosStrategy.fixed(8));
		FsClient secondAdapted = GlobalFsGatewayDriver.createFsAdapter(second, keys, "testFs", CheckpointPosStrategy.fixed(16));

		String text1 = "Hello world, this is some bytes ";
		String text2 = "to be sent through the GlobalFs HTTP interface";

		SerialSupplier<ByteBuf> supplier = SerialSupplier.of(ByteBuf.wrapForReading(text1.getBytes(UTF_8)), ByteBuf.wrapForReading(text2.getBytes(UTF_8)));

		discoveryService.append(testFs, AnnounceData.of(Instant.now().toEpochMilli(), set(firstId, secondId)), keys.getPrivKey())
				.whenResult($ -> System.out.println("Servers announced"))
				.thenCompose($ -> firstAdapted.upload("test.txt"))
				.thenCompose(supplier::streamTo)
				.whenResult($ -> System.out.println("Upload to first server finished"))
				.thenCompose($ -> secondAdapted.download("test.txt"))
				.thenCompose(s -> s.toCollector(ByteBufQueue.collector()))
				.whenResult(s -> System.out.println("  downloaded: " + s.getString(UTF_8)))
				.whenResult(res -> assertEquals(text1 + text2, res.asString(UTF_8)))
				.whenResult($ -> System.out.println("Download from second server finished"))
				.whenComplete(assertComplete());

		eventloop.run();
	}

	@Test
	@Ignore
	public void announceNodes() throws ParseException {
		KeyPair alice = PrivKey.fromString("IGt2RZdSjXaDLoLZn4DvimyjXRm4QNYSiXSip-uUkjzE").computeKeys();
		RepoID aliceTestFs = RepoID.of(alice, "testFs");
		KeyPair bob = PrivKey.fromString("IEioklPt2UgNsAM0UfSFjKMU5JAu0qBm7EMWwoVQG3Wf").computeKeys();
		RepoID bobTestFs = RepoID.of(bob, "testFs");

		AsyncHttpClient client = AsyncHttpClient.create(eventloop);
		DiscoveryService discoveryService = HttpDiscoveryService.create(new InetSocketAddress(9001), client);

		Set<RawServerId> servers = new HashSet<>();

		for (int i = 1; i <= Integer.parseInt(System.getProperty("globalfs.testing.numOfServers")); i++) {
			servers.add(new RawServerId(new InetSocketAddress(8000 + i)));
		}

		eventloop.post(() ->
				Promises.all(
						discoveryService.announce(aliceTestFs, AnnounceData.of(123, servers), alice.getPrivKey()),
						discoveryService.announce(bobTestFs, AnnounceData.of(234, servers), bob.getPrivKey())
				)
						.whenComplete(assertComplete()));

		eventloop.run();
	}
}
