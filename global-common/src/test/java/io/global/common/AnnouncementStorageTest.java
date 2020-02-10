package io.global.common;

import io.datakernel.codec.StructuredCodec;
import io.datakernel.test.rules.ByteBufRule;
import io.datakernel.test.rules.EventloopRule;
import io.global.common.api.AnnounceData;
import io.global.common.api.AnnouncementStorage;
import io.global.common.discovery.MySqlAnnouncementStorage;
import io.global.common.discovery.RocksDbAnnouncementStorage;
import io.global.common.stub.InMemoryAnnouncementStorage;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameter;
import org.junit.runners.Parameterized.Parameters;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;

import javax.sql.DataSource;
import java.io.IOException;
import java.nio.file.Path;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.Collection;
import java.util.function.Function;

import static io.datakernel.common.collection.CollectionUtils.set;
import static io.datakernel.promise.TestUtils.await;
import static io.datakernel.test.TestUtils.dataSource;
import static io.global.common.BinaryDataFormats.REGISTRY;
import static java.util.concurrent.Executors.newCachedThreadPool;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

@RunWith(Parameterized.class)
public class AnnouncementStorageTest {
	private static final StructuredCodec<AnnounceData> ANNOUNCE_DATA_CODEC = REGISTRY.get(AnnounceData.class);
	private static final String MY_SQL_PROPERTIES = "test_announcements.properties";

	@Rule
	public TemporaryFolder temporaryFolder = new TemporaryFolder();

	@ClassRule
	public static final EventloopRule eventloopRule = new EventloopRule();

	@ClassRule
	public static final ByteBufRule byteBufRule = new ByteBufRule();

	@Parameter()
	public Function<Path, AnnouncementStorage> storageFn;

	private AnnouncementStorage storage;

	@Parameters(name = "{0}")
	public static Collection<Object[]> getParameters() {
		return Arrays.asList(
				new Object[]{(Function<Path, AnnouncementStorage>) path -> {
					try {
						return RocksDbAnnouncementStorage.create(newCachedThreadPool(), RocksDB.open(path.toString()));
					} catch (RocksDBException e) {
						throw new AssertionError(e);
					}
				}
				},
				new Object[]{(Function<Path, AnnouncementStorage>) path -> {
					try {
						DataSource dataSource = dataSource(MY_SQL_PROPERTIES);
						return MySqlAnnouncementStorage.create(newCachedThreadPool(), dataSource);
					} catch (IOException | SQLException e) {
						System.out.println("WARNING: Failed to get properties from " + MY_SQL_PROPERTIES + " (" +
								e.getMessage() + "), using stub instead");
						return new InMemoryAnnouncementStorage();
					}
				}
				}
		);
	}

	@Before
	public void setUp() throws IOException, SQLException {
		storage = storageFn.apply(temporaryFolder.newFolder().toPath());
		if (storage instanceof MySqlAnnouncementStorage) {
			((MySqlAnnouncementStorage) storage).initialize();
			((MySqlAnnouncementStorage) storage).truncateTables();
		}
	}

	@Test
	public void testStoreAndLoad() {
		KeyPair keys = KeyPair.generate();
		PubKey pubKey = keys.getPubKey();
		PrivKey privKey = keys.getPrivKey();

		AnnounceData announceData = AnnounceData.of(100, set(
				new RawServerId("First"),
				new RawServerId("Second")));
		SignedData<AnnounceData> announcement = SignedData.sign(ANNOUNCE_DATA_CODEC, announceData, privKey);

		await(storage.store(pubKey, announcement));
		SignedData<AnnounceData> fetchedAnnouncement = await(storage.load(pubKey));

		assertEquals(announcement, fetchedAnnouncement);
	}

	@Test
	public void testLoadEmpty() {
		assertNull(await(storage.load(KeyPair.generate().getPubKey())));
	}

	@Test
	public void testStoreMultipleSameKey() {
		KeyPair keys = KeyPair.generate();
		PubKey pubKey = keys.getPubKey();
		PrivKey privKey = keys.getPrivKey();

		AnnounceData announceData1 = AnnounceData.of(100, set(
				new RawServerId("First"),
				new RawServerId("Second")));
		SignedData<AnnounceData> announcement1 = SignedData.sign(ANNOUNCE_DATA_CODEC, announceData1, privKey);

		AnnounceData announceData2 = AnnounceData.of(50, set(
				new RawServerId("One"),
				new RawServerId("Two")));
		SignedData<AnnounceData> announcement2 = SignedData.sign(ANNOUNCE_DATA_CODEC, announceData2, privKey);

		await(storage.store(pubKey, announcement1));
		await(storage.store(pubKey, announcement2));
		SignedData<AnnounceData> fetchedAnnouncement = await(storage.load(pubKey));

		assertEquals(announcement2, fetchedAnnouncement);
	}


	@Test
	public void testStoreMultipleKeys() {
		KeyPair keys1 = KeyPair.generate();
		PubKey pubKey1 = keys1.getPubKey();
		PrivKey privKey1 = keys1.getPrivKey();

		KeyPair keys2 = KeyPair.generate();
		PubKey pubKey2 = keys2.getPubKey();
		PrivKey privKey2 = keys2.getPrivKey();

		AnnounceData announceData1 = AnnounceData.of(100, set(
				new RawServerId("First"),
				new RawServerId("Second")));
		SignedData<AnnounceData> announcement1 = SignedData.sign(ANNOUNCE_DATA_CODEC, announceData1, privKey1);

		AnnounceData announceData2 = AnnounceData.of(50, set(
				new RawServerId("One"),
				new RawServerId("Two")));
		SignedData<AnnounceData> announcement2 = SignedData.sign(ANNOUNCE_DATA_CODEC, announceData2, privKey2);

		await(storage.store(pubKey1, announcement1));
		await(storage.store(pubKey2, announcement2));
		SignedData<AnnounceData> fetchedAnnouncement1 = await(storage.load(pubKey1));
		SignedData<AnnounceData> fetchedAnnouncement2 = await(storage.load(pubKey2));

		assertEquals(announcement1, fetchedAnnouncement1);
		assertEquals(announcement2, fetchedAnnouncement2);
	}

}
