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

import io.datakernel.async.Promise;
import io.datakernel.bytebuf.ByteBuf;
import io.datakernel.bytebuf.ByteBufQueue;
import io.datakernel.eventloop.Eventloop;
import io.datakernel.exception.ParseException;
import io.datakernel.remotefs.FsClient;
import io.datakernel.remotefs.LocalFsClient;
import io.datakernel.serial.SerialSupplier;
import io.datakernel.stream.processor.ActivePromisesRule;
import io.global.common.KeyPair;
import io.global.common.RawServerId;
import io.global.common.SignedData;
import io.global.common.api.AnnounceData;
import io.global.common.api.DiscoveryService;
import io.global.fs.api.*;
import io.global.fs.http.GlobalFsNodeServlet;
import io.global.fs.http.HttpGlobalFsNode;
import io.global.fs.local.GlobalFsDriver;
import io.global.fs.local.LocalGlobalFsNode;
import io.global.fs.local.RuntimeDiscoveryService;
import io.global.fs.transformers.FrameSigner;
import org.junit.*;
import org.junit.rules.TemporaryFolder;
import org.spongycastle.crypto.digests.SHA256Digest;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.file.Paths;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static io.datakernel.bytebuf.ByteBufStrings.wrapUtf8;
import static io.datakernel.eventloop.FatalErrorHandlers.rethrowOnAnyError;
import static io.datakernel.test.TestUtils.assertComplete;
import static io.datakernel.test.TestUtils.assertFailure;
import static io.datakernel.util.CollectionUtils.list;
import static io.datakernel.util.CollectionUtils.set;
import static io.global.fs.api.CheckpointPosStrategy.fixed;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.stream.Collectors.toList;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class GlobalFsTest {

	@Rule
	public TemporaryFolder temporaryFolder = new TemporaryFolder();

	@Rule
	public ActivePromisesRule activePromisesRule = new ActivePromisesRule();

	private Eventloop eventloop = Eventloop.create().withCurrentThread().withFatalErrorHandler(rethrowOnAnyError());
	private ExecutorService executor = Executors.newSingleThreadExecutor();

	private DiscoveryService discoveryService = new RuntimeDiscoveryService();

	private KeyPair alice = KeyPair.generate();
	private KeyPair bob = KeyPair.generate();

	private RawServerId firstId = new RawServerId(new InetSocketAddress(123));
	private RawServerId secondId = new RawServerId(new InetSocketAddress(124));

	private LocalGlobalFsNode rawFirstClient;
	private LocalGlobalFsNode rawSecondClient;

	private GlobalFsNode firstClient;
	private GlobalFsDriver firstDriver;
	private FsClient firstAliceAdapter;
	private FsClient secondAliceAdapter;

	@Before
	public void setUp() throws IOException, InterruptedException {
		Runtime.getRuntime().exec("rm -r /tmp/TESTS2").waitFor();

		NodeClientFactory clientFactory = new NodeClientFactory() {
			FsClient storage = LocalFsClient.create(eventloop, executor, Paths.get("/tmp/TESTS2/")); //temporaryFolder.newFolder().toPath());

			@Override
			public GlobalFsNode create(RawServerId serverId) {
				return LocalGlobalFsNode.create(serverId, discoveryService, this, storage.subfolder("server_" + serverId.getInetSocketAddress().getPort()))
						.withManagedPubKeys(set(alice.getPubKey(), bob.getPubKey()));
			}
		};

		rawFirstClient = (LocalGlobalFsNode) clientFactory.create(firstId);
		rawSecondClient = (LocalGlobalFsNode) clientFactory.create(secondId);

		firstClient = wrapWithHttpInterface(rawFirstClient);
		GlobalFsNode secondClient = wrapWithHttpInterface(rawSecondClient);

		firstDriver = GlobalFsDriver.create(firstClient, list(alice, bob), fixed(10));
		GlobalFsDriver secondDriver = GlobalFsDriver.create(secondClient, list(alice, bob), fixed(15));

		firstAliceAdapter = firstDriver.createClientFor(alice.getPubKey());
		secondAliceAdapter = secondDriver.createClientFor(alice.getPubKey());
	}

	@After
	public void tearDown() {
		executor.shutdownNow();
	}

	private Promise<Void> announce(KeyPair keys, Set<RawServerId> rawServerIds) {
		return discoveryService.announce(keys.getPubKey(), SignedData.sign(AnnounceData.of(123, rawServerIds), keys.getPrivKey()));
	}

	@Test
	public void cutters() {
		announce(alice, set(firstId))
				.thenCompose($ -> firstAliceAdapter.upload("test1.txt"))
				.thenCompose(SerialSupplier.of(
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
				)::streamTo)
				.thenCompose($ -> firstAliceAdapter.download("test1.txt", 10, 380 - 10 - 19))
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
				.thenCompose($ -> firstAliceAdapter.download("test1.txt", 64, 259))
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
				.thenCompose($ -> firstAliceAdapter.downloadSerial("test1.txt", 228, 37).toCollector(ByteBufQueue.collector()))
				.whenComplete(assertComplete(buf ->
						assertEquals("hello, this is a test buffer data #07", buf.asString(UTF_8))))
				.thenCompose($ -> firstAliceAdapter.downloadSerial("test1.txt").toCollector(ByteBufQueue.collector()))
				.whenComplete(assertComplete(buf -> assertEquals(570, buf.asString(UTF_8).length())));

		eventloop.run();
	}

	@Test
	public void separate() {
		FsClient firstBobAdapter = firstDriver.createClientFor(bob.getPubKey());

		String content = "hello world, i am here!";

		firstAliceAdapter.upload("test.txt")
				.thenCompose(consumer -> SerialSupplier.of(ByteBuf.wrapForReading(content.getBytes(UTF_8))).streamTo(consumer))
				.thenCompose($ -> firstAliceAdapter.download("test.txt"))
				.thenCompose(supplier -> supplier.toCollector(ByteBufQueue.collector()))
				.whenResult(buf -> assertEquals(content, buf.asString(UTF_8)))
				.thenCompose($ -> firstBobAdapter.download("test.txt"))
				.whenComplete(assertFailure());

		eventloop.run();
	}

	@Test
	public void downloadOnlyCheckpoint() {
		String content = "little test content";

		String filename = "folder/test.txt";

		SerialSupplier.of(wrapUtf8(content))
				.apply(FrameSigner.create(alice.getPrivKey(), CheckpointPosStrategy.fixed(4), filename, 0, new SHA256Digest()))
				.streamTo(firstClient.uploader(alice.getPubKey(), filename, -1))
				.thenCompose($ -> firstClient.pushMetadata(alice.getPubKey(),
						SignedData.sign(GlobalFsMetadata.of(filename, content.length(), System.currentTimeMillis()), alice.getPrivKey())))
				.thenCompose($ -> firstClient.download(alice.getPubKey(), filename, 4, 0))
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
		announce(alice, set(firstId))
				.thenCompose($ -> firstAliceAdapter.upload("folder/test.txt"))
				.thenCompose(SerialSupplier.of(wrapUtf8("first line of the content\n"))::streamTo)
				.thenCompose($ ->
						SerialSupplier.of(wrapUtf8("ntent\nsecond line, appended\n"))
								.streamTo(firstAliceAdapter.uploadSerial("folder/test.txt", 20)))
				.thenCompose($ -> firstAliceAdapter.downloadSerial("folder/test.txt").toCollector(ByteBufQueue.collector()))
				.whenResult(buf -> System.out.println(buf.asString(UTF_8)))
				.thenCompose($ -> firstAliceAdapter.getMetadata("folder/test.txt"))
				.whenComplete(assertComplete(meta -> assertEquals(48, meta.getSize())));

		eventloop.run();
	}

	@Test
	public void testAppend() {
		String first = "Hello world, this is some bytes ";
		String second = "to be sent through the GlobalFs HTTP interface";

		announce(alice, set(firstId))
				.thenCompose($ -> firstAliceAdapter.upload("test.txt"))
				.thenCompose(SerialSupplier.of(wrapUtf8(first))::streamTo)
				.thenCompose($ -> firstAliceAdapter.getMetadata("test.txt"))
				.thenCompose(meta -> firstAliceAdapter.upload("test.txt", meta.getSize() - 6))
				.thenCompose(SerialSupplier.of(wrapUtf8("bytes " + second))::streamTo)
				.thenCompose($ -> firstAliceAdapter.download("test.txt"))
				.thenCompose(supplier -> supplier.toCollector(ByteBufQueue.collector()))
				.whenComplete(assertComplete(res -> assertEquals(first + second, res.asString(UTF_8))));

		eventloop.run();
	}

	@Test
	public void testDownloadFromOther() {
		String string = "hello, this is a test little string of bytes";
		announce(alice, set(firstId, secondId))
				.thenCompose($ -> firstAliceAdapter.upload("test.txt"))
				.thenCompose(SerialSupplier.of(wrapUtf8(string))::streamTo)
				.thenCompose($ -> secondAliceAdapter.download("test.txt"))
				.thenCompose(supplier -> supplier.toCollector(ByteBufQueue.collector()))
				.whenComplete(assertComplete(res -> assertEquals(string, res.asString(UTF_8))));

		eventloop.run();
	}

	@Test
	public void testFetch() {
		String string = "hello, this is a test little string of bytes";
		announce(alice, set(firstId, secondId))
				.thenCompose($ -> firstAliceAdapter.upload("test.txt"))
				.thenCompose(SerialSupplier.of(wrapUtf8(string))::streamTo)
				.thenCompose($ -> rawSecondClient.fetch())
				.whenComplete(assertComplete(Assert::assertTrue))
				.thenCompose($ -> secondAliceAdapter.download("test.txt"))
				.thenCompose(supplier -> supplier.toCollector(ByteBufQueue.collector()))
				.whenComplete(assertComplete(res -> System.out.println(res.asString(UTF_8))));

		eventloop.run();
	}

	@Test
	public void testPartialFetch() {
		String part = "hello, this is a test little string of bytes";
		String string = part + "\nwhich has a second line by the way, hello there";
		announce(alice, set(firstId, secondId))
				.thenCompose($ -> secondAliceAdapter.upload("test.txt"))
				.thenCompose(SerialSupplier.of(wrapUtf8(part))::streamTo)
				.thenCompose($ -> firstAliceAdapter.upload("test.txt", 0))
				.thenCompose(SerialSupplier.of(wrapUtf8(string))::streamTo)
				.thenCompose($ -> rawSecondClient.fetch())
				.whenComplete(assertComplete(res -> assertTrue("Fetch did nothing", res)))
				.thenCompose($ -> secondAliceAdapter.download("test.txt"))
				.thenCompose(supplier -> supplier.toCollector(ByteBufQueue.collector()))
				.whenComplete(assertComplete(res -> assertEquals(string, res.asString(UTF_8))));

		eventloop.run();
	}

	@Test
	@Ignore("does not work yet for some reason")
	public void uploadWhenOldCache() {
		announce(alice, set(secondId))
				.thenCompose($ -> firstAliceAdapter.upload("test.txt"))
				.thenCompose(SerialSupplier.of(wrapUtf8("some string of bytes to test"))::streamTo)
				.thenCompose($ -> secondAliceAdapter.delete("test.txt"))
				.thenCompose($ -> rawFirstClient.fetch())
				.thenCompose($ -> firstAliceAdapter.upload("test.txt"))
				.thenCompose(SerialSupplier.of(wrapUtf8("another string of bytes to test"))::streamTo)
				.whenComplete(assertComplete());

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
}
