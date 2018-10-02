/*
 * Copyright (C) 2015-2018  SoftIndex LLC.
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
 *
 */

package io.global.globalfs;

import io.datakernel.bytebuf.ByteBuf;
import io.datakernel.bytebuf.ByteBufQueue;
import io.datakernel.eventloop.Eventloop;
import io.datakernel.http.AsyncHttpClient;
import io.datakernel.http.AsyncHttpServer;
import io.datakernel.remotefs.FsClient;
import io.datakernel.remotefs.LocalFsClient;
import io.datakernel.serial.SerialSupplier;
import io.global.common.KeyPair;
import io.global.common.RawServerId;
import io.global.common.api.AnnounceData;
import io.global.common.api.DiscoveryService;
import io.global.globalfs.api.GlobalFsName;
import io.global.globalfs.api.GlobalFsNode;
import io.global.globalfs.api.NodeFactory;
import io.global.globalfs.http.GlobalFsNodeServlet;
import io.global.globalfs.http.HttpDiscoveryService;
import io.global.globalfs.http.HttpGlobalFsNode;
import io.global.globalfs.local.LocalGlobalFsNode;
import io.global.globalfs.local.RemoteFsAdapter;
import io.global.globalfs.local.RuntimeDiscoveryService;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.time.Duration;
import java.time.Instant;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadLocalRandom;

import static io.datakernel.eventloop.FatalErrorHandlers.rethrowOnAnyError;
import static io.datakernel.test.TestUtils.assertComplete;
import static io.datakernel.test.TestUtils.assertFailure;
import static io.datakernel.util.CollectionUtils.set;
import static java.nio.charset.StandardCharsets.UTF_8;
import static org.junit.Assert.assertEquals;

public class GlobalFsTest {

	@Rule
	public TemporaryFolder temporaryFolder = new TemporaryFolder();

	private Eventloop eventloop;
	private ExecutorService executor;

	private NodeFactory clientFactory;
	private DiscoveryService discoveryService;

	@Before
	public void setUp() throws IOException {
		eventloop = Eventloop.create().withCurrentThread().withFatalErrorHandler(rethrowOnAnyError());
		executor = Executors.newSingleThreadExecutor();
		discoveryService = new RuntimeDiscoveryService();
		FsClient storage = LocalFsClient.create(eventloop, executor, temporaryFolder.newFolder().toPath());
		clientFactory = new NodeFactory() {
			int serverIndex = 0;

			@Override
			public GlobalFsNode create(RawServerId serverId) {
				return LocalGlobalFsNode.create(serverId, discoveryService, this, storage.subfolder("server_" + serverIndex++), () -> Duration.ofMinutes(5));
			}
		};
	}

	@After
	public void tearDown() {
		executor.shutdownNow();
	}

	@Test
	public void testCutters() {
		KeyPair keys = KeyPair.generate();

		GlobalFsNode client = clientFactory.create(new RawServerId(new InetSocketAddress(12345)));
		RemoteFsAdapter adapter = new RemoteFsAdapter(client, GlobalFsName.of(keys, "testFs"), keys, pp -> pp + ThreadLocalRandom.current().nextInt(5, 50));

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
		).streamTo(adapter.uploadSerial("test1.txt", 0))
				.thenCompose($ -> adapter.downloadSerial("test1.txt", 10, 380 - 10 - 19).toCollector(ByteBufQueue.collector()))
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
				.thenCompose($ -> adapter.downloadSerial("test1.txt", 64, 259).toCollector(ByteBufQueue.collector()))
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
				.whenComplete(assertComplete(buf ->
						assertEquals(570, buf.asString(UTF_8).length())));

		eventloop.run();
	}

	@Test
	public void testSeparate() {
		GlobalFsNode client = clientFactory.create(new RawServerId(new InetSocketAddress(12345)));
		KeyPair alice = KeyPair.generate();
		KeyPair bob = KeyPair.generate();

		FsClient adapted = new RemoteFsAdapter(client, GlobalFsName.of(alice, "testFs"), alice, pos -> pos + 3);
		FsClient other = new RemoteFsAdapter(client, GlobalFsName.of(bob, "testFs"), bob, pos -> pos + 4);

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
	public void test() throws IOException {
		KeyPair keys = KeyPair.generate();

		GlobalFsNode node = clientFactory.create(new RawServerId(new InetSocketAddress(12345)));
		AsyncHttpServer server = AsyncHttpServer.create(eventloop, GlobalFsNodeServlet.wrap(node))
				.withListenPort(8080);
		server.listen();

		GlobalFsNode client = new HttpGlobalFsNode(new RawServerId(new InetSocketAddress(8080)), AsyncHttpClient.create(eventloop));
		RemoteFsAdapter adapter = new RemoteFsAdapter(client, GlobalFsName.of(keys, "testFs"), keys, x -> x + 5);

		String first = "Hello world, this is some bytes ";
		String second = "to be sent through the GlobalFs HTTP interface";

		adapter.upload("test.txt")
				.thenCompose(SerialSupplier.of(
						ByteBuf.wrapForReading(first.getBytes(UTF_8)),
						ByteBuf.wrapForReading(second.getBytes(UTF_8)))::streamTo)
				.thenCompose($ -> adapter.downloadSerial("test.txt").toCollector(ByteBufQueue.collector()))
				.whenComplete(assertComplete(res -> assertEquals(first + second, res.asString(UTF_8))))
				.whenComplete(($, e) -> server.close());


		eventloop.run();
	}

	@Test
	public void theTest() throws UnknownHostException {
		KeyPair keys = KeyPair.generate();

		AsyncHttpClient client = AsyncHttpClient.create(eventloop);
		DiscoveryService discoveryService = new HttpDiscoveryService(client, new InetSocketAddress(9001));

		RawServerId firstId = new RawServerId(new InetSocketAddress(InetAddress.getLocalHost(), 8001));
		RawServerId secondId = new RawServerId(new InetSocketAddress(InetAddress.getLocalHost(), 8002));

		GlobalFsNode first = new HttpGlobalFsNode(firstId, client);
		GlobalFsNode second = new HttpGlobalFsNode(secondId, client);

		FsClient firstAdapted = new RemoteFsAdapter(first, GlobalFsName.of(keys, "testFs"), keys, x -> x + 8);
		FsClient secondAdapted = new RemoteFsAdapter(second, GlobalFsName.of(keys, "testFs"), keys, x -> x + 16);

		String text1 = "Hello world, this is some bytes ";
		String text2 = "to be sent through the GlobalFs HTTP interface";

		SerialSupplier<ByteBuf> supplier = SerialSupplier.of(ByteBuf.wrapForReading(text1.getBytes(UTF_8)), ByteBuf.wrapForReading(text2.getBytes(UTF_8)));

		discoveryService.append(keys, AnnounceData.of(Instant.now().toEpochMilli(), keys.getPubKey(), set(firstId, secondId)))
				.whenResult($ -> System.out.println("Servers announced"))
				.thenCompose($ -> firstAdapted.upload("test.txt"))
				.thenCompose(supplier::streamTo)
				.whenResult($ -> System.out.println("Upload to first server finished"))
				.thenCompose($ -> secondAdapted.download("test.txt"))
				.thenCompose(s -> s.toCollector(ByteBufQueue.collector()))
				.whenResult(s -> System.out.println("  downloaded: " + s.getString(UTF_8)))
				.whenResult(res -> assertEquals(text1 + text2, res.asString(UTF_8)))
				.thenRun(() -> System.out.println("Download from second server finished"))
				.whenComplete(assertComplete());

		eventloop.run();
	}
}
