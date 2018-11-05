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
import io.datakernel.remotefs.FsClient;
import io.datakernel.remotefs.LocalFsClient;
import io.datakernel.serial.SerialSupplier;
import io.datakernel.stream.processor.ActivePromisesRule;
import io.global.common.KeyPair;
import io.global.common.PrivKey;
import io.global.common.RawServerId;
import io.global.common.SignedData;
import io.global.common.api.AnnounceData;
import io.global.common.api.DiscoveryService;
import io.global.fs.api.*;
import io.global.fs.http.GlobalFsNodeServlet;
import io.global.fs.http.HttpDiscoveryService;
import io.global.fs.http.HttpGlobalFsNode;
import io.global.fs.local.GlobalFsDriver;
import io.global.fs.local.LocalGlobalFsNode;
import io.global.fs.local.RuntimeDiscoveryService;
import io.global.fs.transformers.FrameSigner;
import io.global.ot.api.RepoID;
import org.junit.*;
import org.junit.rules.TemporaryFolder;
import org.spongycastle.crypto.digests.SHA256Digest;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.file.Paths;
import java.time.Instant;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static io.datakernel.bytebuf.ByteBufStrings.wrapUtf8;
import static io.datakernel.eventloop.FatalErrorHandlers.rethrowOnAnyError;
import static io.datakernel.test.TestUtils.*;
import static io.datakernel.util.CollectionUtils.list;
import static io.datakernel.util.CollectionUtils.set;
import static io.global.fs.api.CheckpointPosStrategy.fixed;
import static io.global.fs.api.CheckpointPosStrategy.randRange;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.stream.Collectors.toList;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class GlobalFsTest {

	@Rule
	public TemporaryFolder temporaryFolder = new TemporaryFolder();

	@Rule
	public ActivePromisesRule activePromisesRule = new ActivePromisesRule();

	private Eventloop eventloop;
	private ExecutorService executor;

	private NodeClientFactory clientFactory;
	private DiscoveryService discoveryService;

	private KeyPair alice, bob;
	private RepoID aliceFs, bobFs;

	@Before
	public void setUp() throws ParseException, IOException, InterruptedException {
		eventloop = Eventloop.create().withCurrentThread().withFatalErrorHandler(rethrowOnAnyError());
		executor = Executors.newSingleThreadExecutor();
		discoveryService = new RuntimeDiscoveryService();

		alice = PrivKey.fromString("d6577f45e352a16e21a29e8b9fb927b17902332c7f141e51a6265558c6bdd7ef").computeKeys();
		aliceFs = RepoID.of(alice, "aliceFs");
		bob = PrivKey.fromString("538451a22387ba099222bdbfdeaed63435fde46c724eb3c72e8c64843c339ea1").computeKeys();
		bobFs = RepoID.of(bob, "bobFs");

		FsClient storage = LocalFsClient.create(eventloop, executor, Paths.get("/tmp/TESTS2/")); //temporaryFolder.newFolder().toPath());

		Runtime.getRuntime().exec("rm -r /tmp/TESTS2").waitFor();

		clientFactory = new NodeClientFactory() {
			@Override
			public GlobalFsNode create(RawServerId serverId) {
				return LocalGlobalFsNode.create(serverId, discoveryService, this, storage.subfolder("server_" + serverId.getInetSocketAddress().getPort()))
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
		GlobalFsNode client = wrapWithHttpInterface(clientFactory.create(new RawServerId(new InetSocketAddress(12345))));

		GlobalFsDriver driver = GlobalFsDriver.create(client, list(alice), randRange(25, 50));
		FsClient adapter = driver.createClientFor(alice.getPubKey());

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

		GlobalFsDriver driver = GlobalFsDriver.create(client, list(alice, bob), CheckpointPosStrategy.fixed(5));
		FsClient adapted = driver.createClientFor(alice.getPubKey());
		FsClient other = driver.createClientFor(bob.getPubKey());

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

		String content = "little test content";

		String filename = "folder/test.txt";

		SerialSupplier.of(wrapUtf8(content))
				.apply(FrameSigner.create(alice.getPrivKey(), CheckpointPosStrategy.fixed(4), filename, 0, new SHA256Digest()))
				.streamTo(client.uploader(alice.getPubKey(), filename, -1))
				.thenCompose($ -> client.pushMetadata(alice.getPubKey(),
						SignedData.sign(GlobalFsMetadata.of(filename, content.length(), System.currentTimeMillis()), alice.getPrivKey())))
				.thenCompose($ -> client.download(alice.getPubKey(), filename, 4, 0))
				.thenCompose(supplier -> supplier.toCollector(toList()))
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

		GlobalFsDriver driver = GlobalFsDriver.create(client, list(alice), fixed(5));
		FsClient gateway = driver.createClientFor(alice.getPubKey());

		SerialSupplier.of(wrapUtf8("first line of the content\n"))
				.streamTo(gateway.uploadSerial("folder/test.txt"))
				.thenCompose($ ->
						SerialSupplier.of(wrapUtf8("ntent\nsecond line, appended\n"))
								.streamTo(gateway.uploadSerial("folder/test.txt", 20)))
				.thenCompose($ -> gateway.downloadSerial("folder/test.txt").toCollector(ByteBufQueue.collector()))
				.whenResult(buf -> System.out.println(buf.asString(UTF_8)))
				.thenCompose($ -> gateway.getMetadata("folder/test.txt"))
				.whenComplete(assertComplete(meta -> assertEquals(48, meta.getSize())));

		eventloop.run();
	}

	@Test
	public void testAppend() {
		GlobalFsNode client = wrapWithHttpInterface(clientFactory.create(new RawServerId(new InetSocketAddress(12345))));

		GlobalFsDriver driver = GlobalFsDriver.create(client, list(alice), fixed(5));
		FsClient adapter = driver.createClientFor(alice.getPubKey());

		String first = "Hello world, this is some bytes ";
		String second = "to be sent through the GlobalFs HTTP interface";

		adapter.upload("test.txt")
				.thenCompose(SerialSupplier.of(wrapUtf8(first))::streamTo)
				.thenCompose($ -> adapter.getMetadata("test.txt"))
				.thenCompose(meta -> adapter.upload("test.txt", meta.getSize() - 6))
				.thenCompose(SerialSupplier.of(wrapUtf8("bytes " + second))::streamTo)
				.thenCompose($ -> adapter.download("test.txt"))
				.thenCompose(supplier -> supplier.toCollector(ByteBufQueue.collector()))
				.whenComplete(assertComplete(res -> assertEquals(first + second, res.asString(UTF_8))));

		eventloop.run();
	}

	@Test
	public void testDownloadFromOther() {
		RawServerId first = new RawServerId(new InetSocketAddress(123));
		RawServerId second = new RawServerId(new InetSocketAddress(124));

		GlobalFsNode firstClient = wrapWithHttpInterface(clientFactory.create(first));
		GlobalFsNode secondClient = wrapWithHttpInterface(clientFactory.create(second));

		GlobalFsDriver firstDriver = GlobalFsDriver.create(firstClient, list(alice), fixed(5));
		GlobalFsDriver secondDriver = GlobalFsDriver.create(secondClient, list(alice), fixed(6));

		FsClient firstAdapter = firstDriver.createClientFor(alice.getPubKey());
		FsClient secondAdapter = secondDriver.createClientFor(alice.getPubKey());

		String string = "hello, this is a test little string of bytes";
		discoveryService.announce(alice.getPubKey(), SignedData.sign(AnnounceData.of(123, set(first, second)), alice.getPrivKey()))
				.thenCompose($ -> firstAdapter.upload("test.txt"))
				.thenCompose(SerialSupplier.of(wrapUtf8(string))::streamTo)
				.thenCompose($ -> secondAdapter.download("test.txt"))
				.thenCompose(supplier -> supplier.toCollector(ByteBufQueue.collector()))
				.whenComplete(assertComplete(res -> assertEquals(string, res.asString(UTF_8))));

		eventloop.run();
	}

	@Test
	public void testFetch() {
		enableLogging();

		RawServerId first = new RawServerId(new InetSocketAddress(123));
		RawServerId second = new RawServerId(new InetSocketAddress(124));

		GlobalFsNode firstClient = wrapWithHttpInterface(clientFactory.create(first));
		LocalGlobalFsNode rawSecondClient = (LocalGlobalFsNode) clientFactory.create(second);
		GlobalFsNode secondClient = wrapWithHttpInterface(rawSecondClient);

		GlobalFsDriver firstDriver = GlobalFsDriver.create(firstClient, list(alice), fixed(5));
		GlobalFsDriver secondDriver = GlobalFsDriver.create(secondClient, list(alice), fixed(6));

		FsClient firstAdapter = firstDriver.createClientFor(alice.getPubKey());
		FsClient secondAdapter = secondDriver.createClientFor(alice.getPubKey());

		String string = "hello, this is a test little string of bytes";
		discoveryService.announce(alice.getPubKey(), SignedData.sign(AnnounceData.of(123, set(first, second)), alice.getPrivKey()))
				.thenCompose($ -> firstAdapter.upload("test.txt"))
				.thenCompose(SerialSupplier.of(wrapUtf8(string))::streamTo)
				.thenCompose($ -> rawSecondClient.fetch())
				.whenComplete(assertComplete(Assert::assertTrue))
				.thenCompose($ -> secondAdapter.download("test.txt"))
				.thenCompose(supplier -> supplier.toCollector(ByteBufQueue.collector()))
				.whenComplete(assertComplete(res -> System.out.println(res.asString(UTF_8))));

		eventloop.run();
	}

	@Test
	public void testFetchLarger() {
		RawServerId first = new RawServerId(new InetSocketAddress(123));
		RawServerId second = new RawServerId(new InetSocketAddress(124));

		GlobalFsNode firstClient = wrapWithHttpInterface(clientFactory.create(first));
		LocalGlobalFsNode rawSecondClient = (LocalGlobalFsNode) clientFactory.create(second);
		GlobalFsNode secondClient = wrapWithHttpInterface(rawSecondClient);

		GlobalFsDriver firstDriver = GlobalFsDriver.create(firstClient, list(alice), fixed(5));
		GlobalFsDriver secondDriver = GlobalFsDriver.create(secondClient, list(alice), fixed(6));

		FsClient firstAdapter = firstDriver.createClientFor(alice.getPubKey());
		FsClient secondAdapter = secondDriver.createClientFor(alice.getPubKey());

		String part = "hello, this is a test little string of bytes";
		String string = part + "\nwhich has a second line by the way, hello there";
		discoveryService.announce(alice.getPubKey(), SignedData.sign(AnnounceData.of(123, set(first, second)), alice.getPrivKey()))
				.thenCompose($ -> secondAdapter.upload("test.txt"))
				.thenCompose(SerialSupplier.of(wrapUtf8(part))::streamTo)
				.thenCompose($ -> firstAdapter.upload("test.txt"))
				.thenCompose(SerialSupplier.of(wrapUtf8(string))::streamTo)
				.thenCompose($ -> rawSecondClient.fetch())
				.whenComplete(assertComplete(res -> assertTrue("Fetch did nothing", res)))
				.thenCompose($ -> secondAdapter.download("test.txt"))
				.thenCompose(supplier -> supplier.toCollector(ByteBufQueue.collector()))
				.whenComplete(assertComplete(res -> assertEquals(string, res.asString(UTF_8))));

		eventloop.run();
	}

	private GlobalFsNode wrapWithHttpInterface(GlobalFsNode node) {
		GlobalFsNodeServlet servlet = new GlobalFsNodeServlet(node);
		return new HttpGlobalFsNode(request -> {
			try {
				return servlet.serve(request);
			} catch (ParseException e) {
				throw new AssertionError(e);
			}
		}, new InetSocketAddress(123));
	}

	@Test
	@Ignore // requires launched discovery service and two nodes
	public void uploadDownload() {
		AsyncHttpClient client = AsyncHttpClient.create(eventloop);
		DiscoveryService discoveryService = HttpDiscoveryService.create(new InetSocketAddress(9001), client);

		RawServerId firstId = new RawServerId(new InetSocketAddress(8001));
		RawServerId secondId = new RawServerId(new InetSocketAddress(8002));

		GlobalFsNode firstClient = new HttpGlobalFsNode(client, firstId.getInetSocketAddress());
		GlobalFsNode secondClient = new HttpGlobalFsNode(client, secondId.getInetSocketAddress());

		GlobalFsDriver firstDriver = GlobalFsDriver.create(firstClient, list(alice), fixed(5));
		GlobalFsDriver secondDriver = GlobalFsDriver.create(secondClient, list(alice), fixed(6));

		FsClient firstAdapter = firstDriver.createClientFor(alice.getPubKey());
		FsClient secondAdapter = secondDriver.createClientFor(alice.getPubKey());

		String text1 = "Hello world, this is some bytes ";
		String text2 = "to be sent through the GlobalFs HTTP interface";

		SerialSupplier<ByteBuf> supplier = SerialSupplier.of(ByteBuf.wrapForReading(text1.getBytes(UTF_8)), ByteBuf.wrapForReading(text2.getBytes(UTF_8)));

		discoveryService.announce(alice.getPubKey(), SignedData.sign(AnnounceData.of(Instant.now().toEpochMilli(), set(firstId, secondId)), alice.getPrivKey()))
				.whenResult($ -> System.out.println("Servers announced"))
				.thenCompose($ -> firstAdapter.upload("test.txt"))
				.thenCompose(supplier::streamTo)
				.whenResult($ -> System.out.println("Upload to first server finished"))
				.thenCompose($ -> secondAdapter.download("test.txt"))
				.thenCompose(s -> s.toCollector(ByteBufQueue.collector()))
				.whenResult(s -> System.out.println("  downloaded: " + s.getString(UTF_8)))
				.whenResult(res -> assertEquals(text1 + text2, res.asString(UTF_8)))
				.whenResult($ -> System.out.println("Download from second server finished"))
				.whenComplete(assertComplete());

		eventloop.run();
	}

	@Test
	@Ignore
	public void announceNodes() {

		AsyncHttpClient client = AsyncHttpClient.create(eventloop);
		DiscoveryService discoveryService = HttpDiscoveryService.create(new InetSocketAddress(9001), client);

		Set<RawServerId> servers = new HashSet<>();

		for (int i = 1; i <= Integer.parseInt(System.getProperty("globalfs.testing.numOfServers")); i++) {
			servers.add(new RawServerId(new InetSocketAddress(8000 + i)));
		}

		eventloop.post(() ->
				Promises.all(
						discoveryService.announce(alice.getPubKey(), SignedData.sign(AnnounceData.of(123, servers), alice.getPrivKey())),
						discoveryService.announce(bob.getPubKey(), SignedData.sign(AnnounceData.of(234, servers), bob.getPrivKey()))
				)
						.whenComplete(assertComplete()));

		eventloop.run();
	}
}
