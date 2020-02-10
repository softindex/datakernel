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

import io.datakernel.bytebuf.ByteBuf;
import io.datakernel.bytebuf.ByteBufQueue;
import io.datakernel.codec.StructuredCodec;
import io.datakernel.common.parse.ParseException;
import io.datakernel.csp.ChannelConsumer;
import io.datakernel.csp.ChannelSupplier;
import io.datakernel.eventloop.Eventloop;
import io.datakernel.http.AsyncHttpClient;
import io.datakernel.http.StubHttpClient;
import io.datakernel.remotefs.FsClient;
import io.datakernel.remotefs.LocalFsClient;
import io.datakernel.test.rules.ByteBufRule;
import io.datakernel.test.rules.EventloopRule;
import io.global.common.KeyPair;
import io.global.common.PrivKey;
import io.global.common.PubKey;
import io.global.common.RawServerId;
import io.global.common.api.AnnounceData;
import io.global.common.api.AnnouncementStorage;
import io.global.common.api.DiscoveryService;
import io.global.common.discovery.*;
import io.global.common.stub.InMemoryAnnouncementStorage;
import io.global.fs.api.GlobalFsNode;
import io.global.fs.http.HttpGlobalFsNode;
import io.global.fs.local.GlobalFsDriver;
import org.junit.*;
import org.junit.rules.TemporaryFolder;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;

import javax.sql.DataSource;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.file.Path;
import java.sql.SQLException;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Executors;

import static io.datakernel.common.collection.CollectionUtils.set;
import static io.datakernel.eventloop.Eventloop.getCurrentEventloop;
import static io.datakernel.http.AsyncHttpClient.create;
import static io.datakernel.promise.TestUtils.await;
import static io.datakernel.test.TestUtils.dataSource;
import static io.global.common.BinaryDataFormats.REGISTRY;
import static io.global.common.SignedData.sign;
import static io.global.fs.api.CheckpointPosStrategy.of;
import static java.lang.Integer.parseInt;
import static java.lang.System.getProperty;
import static java.nio.charset.StandardCharsets.UTF_8;
import static org.junit.Assert.assertEquals;

@Ignore("setting up methods that require running instances of servers")
public final class GlobalFsSetup {
	public static final StructuredCodec<AnnounceData> ANNOUNCE_DATA_CODEC = REGISTRY.get(AnnounceData.class);
	private static final String MY_SQL_PROPERTIES = "test_announcements.properties";

	@ClassRule
	public static final EventloopRule eventloopRule = new EventloopRule();

	@Rule
	public final TemporaryFolder tmpFolder = new TemporaryFolder();

	@ClassRule
	public static final ByteBufRule byteBufRule = new ByteBufRule();

	private KeyPair alice, bob;

	@Before
	public void setUp() throws ParseException {
		alice = PrivKey.fromString("d6577f45e352a16e21a29e8b9fb927b17902332c7f141e51a6265558c6bdd7ef").computeKeys();
		bob = PrivKey.fromString("538451a22387ba099222bdbfdeaed63435fde46c724eb3c72e8c64843c339ea1").computeKeys();
	}

	@Ignore
	@Test
	public void uploadDownload() {
		AsyncHttpClient client = AsyncHttpClient.create(Eventloop.getCurrentEventloop());
		DiscoveryService discoveryService = HttpDiscoveryService.create("http://127.0.0.1:9001/", client);

		String firstAddress = "http://127.0.0.1:8001/";
		String secondAddress = "http://127.0.0.1:8002/";
		RawServerId first = new RawServerId(firstAddress);
		RawServerId second = new RawServerId(secondAddress);

		GlobalFsNode firstClient = HttpGlobalFsNode.create(firstAddress, client);
		GlobalFsNode secondClient = HttpGlobalFsNode.create(secondAddress, client);

		GlobalFsDriver firstDriver = GlobalFsDriver.create(firstClient, of(5));
		GlobalFsDriver secondDriver = GlobalFsDriver.create(secondClient, of(6));

		FsClient firstAdapter = firstDriver.adapt(alice.getPubKey());
		FsClient secondAdapter = secondDriver.adapt(alice.getPubKey());

		String text1 = "Hello world, this is some bytes ";
		String text2 = "to be sent through the GlobalFs HTTP interface";

		ChannelSupplier<ByteBuf> supplier = ChannelSupplier.of(ByteBuf.wrapForReading(text1.getBytes(UTF_8)), ByteBuf.wrapForReading(text2.getBytes(UTF_8)));

		await(discoveryService.announce(alice.getPubKey(), sign(ANNOUNCE_DATA_CODEC, AnnounceData.of(123, set(first, second)), alice.getPrivKey())));
		System.out.println("Servers announced");

		ChannelConsumer<ByteBuf> channelConsumer = await(firstAdapter.upload("test.txt"));
		await(supplier.streamTo(channelConsumer));
		System.out.println("Upload to first server finished");

		ChannelSupplier<ByteBuf> channelSupplier = await(secondAdapter.download("test.txt"));
		ByteBuf downloaded = await(channelSupplier.toCollector(ByteBufQueue.collector()));
		System.out.println("  downloaded: " + downloaded.getString(UTF_8));
		assertEquals(text1 + text2, downloaded.asString(UTF_8));
		System.out.println("Download from second server finished");
	}

	@Test
	public void findAllTestHttpDiscoveryWithRemoteFsAnnouncement() throws IOException {
		LocalFsClient storage = LocalFsClient.create(getCurrentEventloop(), tmpFolder.newFolder("storage").toPath()).withRevisions();
		RemoteFsSharedKeyStorage sharedKeyStorage = new RemoteFsSharedKeyStorage(storage.subfolder("keys"));
		testDiscoveryService(new RemoteFsAnnouncementStorage(storage.subfolder("announcements")), sharedKeyStorage);
	}

	@Test
	public void findAllTestHttpDiscoveryWithRocksDbAnnouncement() throws IOException, RocksDBException {
		LocalFsClient storage = LocalFsClient.create(getCurrentEventloop(), tmpFolder.newFolder("storage").toPath()).withRevisions();
		RemoteFsSharedKeyStorage sharedKeyStorage = new RemoteFsSharedKeyStorage(storage.subfolder("keys"));
		Path rocksDbPath = tmpFolder.newFolder("rocksDb").toPath();
		testDiscoveryService(RocksDbAnnouncementStorage.create(Executors.newSingleThreadExecutor(), RocksDB.open(rocksDbPath.toString())), sharedKeyStorage);
	}

	@Test
	public void findAllTestHttpDiscoveryWithInMemory() throws IOException {
		LocalFsClient storage = LocalFsClient.create(getCurrentEventloop(), tmpFolder.newFolder("storage").toPath()).withRevisions();
		RemoteFsSharedKeyStorage sharedKeyStorage = new RemoteFsSharedKeyStorage(storage.subfolder("keys"));
		testDiscoveryService(new InMemoryAnnouncementStorage(), sharedKeyStorage);
	}

	@Ignore
	@Test
	public void findAllTestHttpDiscoveryMySQL() throws IOException, SQLException {
		LocalFsClient storage = LocalFsClient.create(getCurrentEventloop(), tmpFolder.newFolder("storage").toPath()).withRevisions();
		RemoteFsSharedKeyStorage sharedKeyStorage = new RemoteFsSharedKeyStorage(storage.subfolder("keys"));
		DataSource dataSource = dataSource(MY_SQL_PROPERTIES);
		testDiscoveryService(MySqlAnnouncementStorage.create(Executors.newSingleThreadExecutor(), dataSource), sharedKeyStorage);
	}

	private void testDiscoveryService(AnnouncementStorage announcementStorage, RemoteFsSharedKeyStorage sharedKeyStorage) {
		LocalDiscoveryService localDiscoveryService = LocalDiscoveryService.create(Eventloop.getCurrentEventloop(), announcementStorage, sharedKeyStorage);
		StubHttpClient stubClient = StubHttpClient.of(DiscoveryServlet.create(localDiscoveryService));
		HttpDiscoveryService httpDiscoveryService = HttpDiscoveryService.create("http://127.0.0.1:7000", stubClient, "admin", "admin");

		String firstAddress = "http://127.0.0.1:8002/";
		String secondAddress = "http://127.0.0.1:8003/";
		RawServerId first = new RawServerId(firstAddress);
		RawServerId second = new RawServerId(secondAddress);

		long time = System.currentTimeMillis();
		await(httpDiscoveryService.announce(alice.getPubKey(), sign(ANNOUNCE_DATA_CODEC, AnnounceData.of(time, set(first, second)), alice.getPrivKey())));
		await(httpDiscoveryService.announce(bob.getPubKey(), sign(ANNOUNCE_DATA_CODEC, AnnounceData.of(time, set(second)), bob.getPrivKey())));

		Map<PubKey, Set<RawServerId>> map = await(httpDiscoveryService.findAll());
		assertEquals(map.get(alice.getPubKey()), set(first, second));
		assertEquals(map.get(bob.getPubKey()), set(second));
	}

	@Ignore
	@Test
	public void announceNodes() {
		AsyncHttpClient client = create(getCurrentEventloop());
		DiscoveryService discoveryService = HttpDiscoveryService.create("http://127.0.0.1:9001/", client);

		Set<RawServerId> servers = new HashSet<>();

		for (int i = 1; i <= parseInt(getProperty("globalfs.testing.numOfServers")); i++) {
			servers.add(new RawServerId("127.0.0.1:" + (8000 + i)));
		}

		await(discoveryService.announce(alice.getPubKey(), sign(ANNOUNCE_DATA_CODEC, AnnounceData.of(123, servers), alice.getPrivKey())));
		await(discoveryService.announce(bob.getPubKey(), sign(ANNOUNCE_DATA_CODEC, AnnounceData.of(234, servers), bob.getPrivKey())));
	}
}
