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
import io.datakernel.csp.ChannelSupplier;
import io.datakernel.eventloop.Eventloop;
import io.datakernel.exception.ParseException;
import io.datakernel.remotefs.FileMetadata;
import io.datakernel.remotefs.FsClient;
import io.datakernel.remotefs.LocalFsClient;
import io.datakernel.stream.processor.ByteBufRule.IgnoreLeaks;
import io.datakernel.stream.processor.DatakernelRunner;
import io.datakernel.stream.processor.LoggingRule.LoggerConfig;
import io.datakernel.stream.processor.Manual;
import io.global.common.*;
import io.global.common.api.AnnounceData;
import io.global.common.api.DiscoveryService;
import io.global.common.api.NodeFactory;
import io.global.common.discovery.LocalDiscoveryService;
import io.global.fs.api.CheckpointPosStrategy;
import io.global.fs.api.DataFrame;
import io.global.fs.api.GlobalFsNode;
import io.global.fs.http.GlobalFsNodeServlet;
import io.global.fs.http.HttpGlobalFsNode;
import io.global.fs.local.GlobalFsDriver;
import io.global.fs.local.LocalGlobalFsNode;
import io.global.fs.transformers.FrameSigner;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.spongycastle.crypto.digests.SHA256Digest;

import java.io.IOException;
import java.nio.file.Path;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static io.datakernel.bytebuf.ByteBufStrings.wrapUtf8;
import static io.datakernel.remotefs.FsClient.FILE_NOT_FOUND;
import static io.datakernel.util.CollectionUtils.list;
import static io.datakernel.util.CollectionUtils.set;
import static io.global.common.TestUtils.await;
import static io.global.common.TestUtils.awaitException;
import static io.global.common.api.SharedKeyStorage.NO_SHARED_KEY;
import static io.global.fs.api.CheckpointPosStrategy.fixed;
import static io.global.fs.api.GlobalFsNode.UPLOADING_TO_TOMBSTONE;
import static io.global.fs.util.BinaryDataFormats.REGISTRY;
import static java.nio.charset.StandardCharsets.UTF_8;
import static org.junit.Assert.*;

@RunWith(DatakernelRunner.class)
public final class GlobalFsTest {

	@Rule
	public TemporaryFolder temporaryFolder = new TemporaryFolder();

	private ExecutorService executor = Executors.newSingleThreadExecutor();

	private DiscoveryService discoveryService;

	private KeyPair alice = KeyPair.generate();
	private KeyPair bob = KeyPair.generate();

	private RawServerId firstId = new RawServerId("127.0.0.1:1234");
	private RawServerId secondId = new RawServerId("127.0.0.1:1235");

	private LocalGlobalFsNode rawFirstClient;
	private LocalGlobalFsNode rawSecondClient;

	private GlobalFsNode firstClient;
	private GlobalFsDriver firstDriver;
	private FsClient firstAliceAdapter;
	private FsClient secondAliceAdapter;
	private FsClient storage;

	@Before
	public void setUp() throws IOException {
		Path dir = temporaryFolder.newFolder().toPath();
		System.out.println("DIR: " + dir);

		storage = LocalFsClient.create(Eventloop.getCurrentEventloop(), executor, dir);
		discoveryService = LocalDiscoveryService.create(Eventloop.getCurrentEventloop(), storage.subfolder("discovery"));

		NodeFactory<GlobalFsNode> clientFactory = new NodeFactory<GlobalFsNode>() {
			@Override
			public GlobalFsNode create(RawServerId serverId) {
				return LocalGlobalFsNode.create(serverId, discoveryService, this, storage.subfolder("server_" + serverId.getServerIdString().split(":")[1]));
			}
		};

		rawFirstClient = (LocalGlobalFsNode) clientFactory.create(firstId);
		rawSecondClient = (LocalGlobalFsNode) clientFactory.create(secondId);

		firstClient = wrapWithHttpInterface(rawFirstClient);
		GlobalFsNode secondClient = wrapWithHttpInterface(rawSecondClient);

		firstDriver = GlobalFsDriver.create(firstClient, discoveryService, list(alice, bob), fixed(10));
		GlobalFsDriver secondDriver = GlobalFsDriver.create(secondClient, discoveryService, list(alice, bob), fixed(15));

		firstAliceAdapter = firstDriver.gatewayFor(alice.getPubKey());
		secondAliceAdapter = secondDriver.gatewayFor(alice.getPubKey());
	}

	private Promise<Void> announce(KeyPair keys, Set<RawServerId> rawServerIds) {
		return discoveryService.announce(keys.getPubKey(), SignedData.sign(REGISTRY.get(AnnounceData.class), AnnounceData.of(123, rawServerIds), keys.getPrivKey()));
	}

	@Test
	public void cutters() {
		await(announce(alice, set(firstId)));

		await(ChannelSupplier.of(
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
		).streamTo(await(firstAliceAdapter.upload("test1.txt"))));


		String res = await(await(firstAliceAdapter.download("test1.txt", 10, 380 - 10 - 19)).toCollector(ByteBufQueue.collector())).asString(UTF_8);

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
				res);

		res = await(await(firstAliceAdapter.download("test1.txt", 64, 259)).toCollector(ByteBufQueue.collector())).asString(UTF_8);
		assertEquals("er data #02\n" +
						"hello, this is a test buffer data #03\n" +
						"hello, this is a test buffer data #04\n" +
						"hello, this is a test buffer data #05\n" +
						"hello, this is a test buffer data #06\n" +
						"hello, this is a test buffer data #07\n" +
						"hello, this is a test buffer data #08\n" +
						"hello, this is a te",
				res);

		res = await(await(firstAliceAdapter.download("test1.txt", 228, 37)).toCollector(ByteBufQueue.collector())).asString(UTF_8);
		assertEquals("hello, this is a test buffer data #07", res);

		res = await(await(firstAliceAdapter.download("test1.txt")).toCollector(ByteBufQueue.collector())).asString(UTF_8);
		assertEquals(570, res.length());
	}

	@Test
	public void separate() {
		FsClient firstBobAdapter = firstDriver.gatewayFor(bob.getPubKey());

		String content = "hello world, i am here!";

		await(ChannelSupplier.of(ByteBuf.wrapForReading(content.getBytes(UTF_8))).streamTo(await(firstAliceAdapter.upload("test.txt"))));

		String res = await(await(firstAliceAdapter.download("test.txt")).toCollector(ByteBufQueue.collector())).asString(UTF_8);
		assertEquals(content, res);

		assertSame(FILE_NOT_FOUND, awaitException(firstBobAdapter.download("test.txt")));
	}

	@Test
	public void downloadOnlyCheckpoint() {
		String content = "little test content";

		String filename = "folder/test.txt";

		await(ChannelSupplier.of(wrapUtf8(content))
				.transformWith(FrameSigner.create(alice.getPrivKey(), CheckpointPosStrategy.fixed(4), filename, 0, new SHA256Digest(), null))
				.streamTo(await(firstClient.upload(alice.getPubKey(), filename, -1))));

		List<DataFrame> list = await(await(firstClient.download(alice.getPubKey(), filename, 4, 0)).toList());
		assertEquals(1, list.size());
		DataFrame frame = list.get(0);
		assertTrue(frame.isCheckpoint());
		assertTrue(frame.getCheckpoint().verify(alice.getPubKey()));
		assertEquals(4, frame.getCheckpoint().getValue().getPosition());
	}

	@Test
	public void uploadWithOffset() {
		await(announce(alice, set(firstId)));

		await(ChannelSupplier.of(wrapUtf8("first line of the content\n")).streamTo(await(firstAliceAdapter.upload("folder/test.txt"))));
		await(ChannelSupplier.of(wrapUtf8("ntent\nsecond line, appended\n")).streamTo(await(firstAliceAdapter.upload("folder/test.txt", 20))));

		String res = await(await(firstAliceAdapter.download("folder/test.txt")).toCollector(ByteBufQueue.collector())).asString(UTF_8);
		assertEquals("first line of the content\nsecond line, appended\n", res);

		FileMetadata meta = await(firstAliceAdapter.getMetadata("folder/test.txt"));
		assertEquals(48, meta.getSize());
	}

	@Test
	public void append() {
		String first = "Hello world, this is some bytes ";
		String second = "to be sent through the GlobalFs HTTP interface";

		await(announce(alice, set(firstId)));
		await(ChannelSupplier.of(wrapUtf8(first)).streamTo(await(firstAliceAdapter.upload("test.txt"))));
		FileMetadata meta = await(firstAliceAdapter.getMetadata("test.txt"));

		await(ChannelSupplier.of(wrapUtf8("bytes " + second)).streamTo(await(firstAliceAdapter.upload("test.txt", meta.getSize() - 6))));
		String res = await(await(firstAliceAdapter.download("test.txt")).toCollector(ByteBufQueue.collector())).asString(UTF_8);
		assertEquals(first + second, res);
	}

	@Test
	public void downloadFromOther() {
		String string = "hello, this is a test little string of bytes";

		// make first node master for alice
		await(announce(alice, set(firstId)));

		// upload a file to the first node
		await(ChannelSupplier.of(wrapUtf8(string)).streamTo(await(firstAliceAdapter.upload("test.txt"))));

		// and the download it from the second
		String res = await(await(secondAliceAdapter.download("test.txt")).toCollector(ByteBufQueue.collector())).asString(UTF_8);
		assertEquals(string, res);
	}

	@Test
	public void fetch() {
		String data = "hello, this is a test little string of bytes";

		await(announce(alice, set(firstId, secondId)));

		// upload file to the first node
		await(ChannelSupplier.of(wrapUtf8(data)).streamTo(await(firstAliceAdapter.upload("test.txt"))));

		// just ping the second node with alice's pubkey so it would check if it it's master
		await(secondAliceAdapter.list());

		// // second node never even heard of alice to know it is it's master
		// rawSecondClient.withManagedPubKeys(set(alice.getPubKey()));

		// make second one catch up
		await(rawSecondClient.catchUp());

		// now second should have this file too
		String res = await(await(secondAliceAdapter.download("test.txt")).toCollector(ByteBufQueue.collector())).asString(UTF_8);
		assertEquals(data, res);
	}

	@Test
	public void partialFetch() {
		String part = "hello, this is a test little string of bytes";
		String string = part + "\nwhich has a second line by the way, hello there";

		await(announce(alice, set(firstId, secondId)));

		// upload half of the text to the second node
		await(ChannelSupplier.of(wrapUtf8(part)).streamTo(await(secondAliceAdapter.upload("test.txt"))));

		// and the whole text to the first one
		await(ChannelSupplier.of(wrapUtf8(string)).streamTo(await(firstAliceAdapter.upload("test.txt", 0))));

		// make second one catch up
		await(rawSecondClient.catchUp());

		// now second should have the whole text too
		String res = await(await(secondAliceAdapter.download("test.txt")).toCollector(ByteBufQueue.collector())).asString(UTF_8);
		assertEquals(string, res);
	}

	@Test
	@IgnoreLeaks("TODO") // TODO anton: fix this
	public void encryptionAndDriver() {
		SimKey key1 = SimKey.generate();
		SimKey key2 = SimKey.generate();

		firstDriver.getPrivateKeyStorage().changeCurrentSimKey(key1);

		String filename = "test.txt";
		String data = "some plain ASCII data to be uploaded and encrypted";

		// upload on first
		await(ChannelSupplier.of(wrapUtf8(data)).streamTo(await(firstAliceAdapter.upload(filename))));

		// and download back
		String res = await(await(firstAliceAdapter.download(filename, 12, 32)).toCollector(ByteBufQueue.collector())).asString(UTF_8);

		// check that encryption-decryption worked
		assertEquals(data.substring(12, 12 + 32), res);

		// pretend we try to download "someone else's" file
		firstDriver.getPrivateKeyStorage().forget(Hash.sha1(key1.getBytes()));
		firstDriver.getPrivateKeyStorage().changeCurrentSimKey(key2);
		assertSame(NO_SHARED_KEY, awaitException(firstAliceAdapter.download(filename)));

		// share "someone else's" file key with outselves
		await(discoveryService.shareKey(alice.getPubKey(),
				SignedData.sign(REGISTRY.get(SharedSimKey.class), SharedSimKey.of(key1, alice.getPubKey()), alice.getPrivKey())));

		// and now we can download
		res = await(await(firstAliceAdapter.download(filename)).toCollector(ByteBufQueue.collector())).asString(UTF_8);

		// and decryption works
		assertEquals(data, res);
	}

	@Test
	@LoggerConfig(logger = "io.global.fs", value = "TRACE")
	@LoggerConfig(logger = "io.global.fs.local.RemoteFsCheckpointStorage", value = "INFO")
	@Ignore("does not work yet") // TODO anton: FIX FORWARDING CONSTANT EXCEPTIONS THROUGH HTTP
	public void fetchDeletions() {
		String filename = "test.txt";

		await(announce(alice, set(firstId, secondId)));

		// uploading one string of bytes to first node
		await(ChannelSupplier.of(wrapUtf8("some string of bytes to test")).streamTo(await(firstAliceAdapter.upload(filename))));

		// deleting the file on second node
		await(secondAliceAdapter.delete(filename));

		// make second node catch up
		await(rawFirstClient.catchUp());

		// uploading on the first node again
		Throwable e = awaitException(firstAliceAdapter.upload(filename));

		assertSame(UPLOADING_TO_TOMBSTONE, e);
	}

	@Test
	public void cacheUpdate() {
		String data = "some string of bytes to test";

		await(announce(alice, set(firstId)));

		// upload to first
		await(ChannelSupplier.of(wrapUtf8(data)).streamTo(await(firstAliceAdapter.upload("test.txt"))));

		// download and cache on second
		String res = await(await(secondAliceAdapter.download("test.txt")).toCollector(ByteBufQueue.collector())).asString(UTF_8);
		assertEquals(data, res);

		// delete on first
		await(firstAliceAdapter.delete("test.txt"));

		// second should check the actual metadata when trying to download
		assertSame(FILE_NOT_FOUND, awaitException(secondAliceAdapter.download("test.txt")));
	}

	@Test
	@Manual("does not work")
	@LoggerConfig(logger = "io.global.fs", value = "TRACE")
	public void proxyUpload() {
		String data = "some string of bytes to test";

		// first node is master
		await(announce(alice, set(firstId)));

		// upload to master through second node
		await(ChannelSupplier.of(wrapUtf8(data)).streamTo(await(secondAliceAdapter.upload("test.txt"))));

		// download from first
		String res = await(await(firstAliceAdapter.download("test.txt")).toCollector(ByteBufQueue.collector())).asString(UTF_8);
		assertEquals(data, res);
	}

	private GlobalFsNode wrapWithHttpInterface(GlobalFsNode node) {
		GlobalFsNodeServlet servlet = new GlobalFsNodeServlet(node);
		return new HttpGlobalFsNode(request -> {
			try {
				return servlet.serve(request);
			} catch (ParseException e) {
				throw new AssertionError(e);
			}
		}, "http://127.0.0.1:3333");
	}
}
