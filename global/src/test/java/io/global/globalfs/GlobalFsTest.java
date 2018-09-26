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
import io.global.common.api.DiscoveryService;
import io.global.globalfs.api.GlobalFsName;
import io.global.globalfs.api.GlobalFsNode;
import io.global.globalfs.api.RawNodeFactory;
import io.global.globalfs.http.GlobalFsNodeServlet;
import io.global.globalfs.http.HttpGlobalFsNode;
import io.global.globalfs.local.LocalGlobalFsNode;
import io.global.globalfs.local.RemoteFsAdapter;
import io.global.globalfs.local.RemoteFsFileSystem;
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
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadLocalRandom;

import static io.datakernel.eventloop.FatalErrorHandlers.rethrowOnAnyError;
import static io.datakernel.test.TestUtils.assertComplete;
import static io.datakernel.test.TestUtils.assertFailure;
import static java.nio.charset.StandardCharsets.UTF_8;
import static org.junit.Assert.assertEquals;

public class GlobalFsTest {

	@Rule
	public TemporaryFolder temporaryFolder = new TemporaryFolder();

	private Eventloop eventloop;
	private ExecutorService executor;

	private RawNodeFactory clientFactory;
	private DiscoveryService discoveryService;

	private RawServerId createLocalhost(int port) {
		try {
			return new RawServerId(new InetSocketAddress(InetAddress.getLocalHost(), port));
		} catch (UnknownHostException e) {
			throw new AssertionError(e);
		}
	}

	@Before
	public void setUp() throws IOException {
		eventloop = Eventloop.create().withCurrentThread().withFatalErrorHandler(rethrowOnAnyError());
		executor = Executors.newSingleThreadExecutor();
		discoveryService = new RuntimeDiscoveryService();
		FsClient storage = LocalFsClient.create(eventloop, executor, temporaryFolder.newFolder().toPath());
		clientFactory = new RawNodeFactory() {
			int serverIndex = 0;

			@Override
			public GlobalFsNode create(RawServerId serverId) {
				LocalGlobalFsNode.FileSystemFactory fileSystemFactory = RemoteFsFileSystem.usingSingleClient(storage.subfolder("server_" + serverIndex++));
				return new LocalGlobalFsNode(serverId, discoveryService, this, fileSystemFactory, () -> Duration.ofMinutes(5));
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

		GlobalFsNode client = clientFactory.create(createLocalhost(12345));
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
		GlobalFsNode client = clientFactory.create(createLocalhost(12345));
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

		GlobalFsNode node = clientFactory.create(createLocalhost(12345));
		AsyncHttpServer server = AsyncHttpServer.create(eventloop, GlobalFsNodeServlet.wrap(node))
				.withListenPort(8080);
		server.listen();

		GlobalFsNode client = new HttpGlobalFsNode(AsyncHttpClient.create(eventloop), "http://127.0.0.1:8080");

		// TODO anton: make this test again

		eventloop.run();
	}
}
