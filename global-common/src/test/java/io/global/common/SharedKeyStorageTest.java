package io.global.common;

import io.datakernel.codec.StructuredCodec;
import io.datakernel.stream.processor.DatakernelRunner.DatakernelRunnerFactory;
import io.global.common.api.SharedKeyStorage;
import io.global.common.discovery.MySqlSharedKeyStorage;
import io.global.common.discovery.RocksDbSharedKeyStorage;
import io.global.common.stub.InMemorySharedKeyStorage;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameter;
import org.junit.runners.Parameterized.Parameters;
import org.junit.runners.Parameterized.UseParametersRunnerFactory;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;

import javax.sql.DataSource;
import java.io.IOException;
import java.nio.file.Path;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.Collection;
import java.util.function.Function;

import static io.datakernel.async.TestUtils.await;
import static io.datakernel.test.TestUtils.dataSource;
import static io.global.common.BinaryDataFormats.REGISTRY;
import static org.junit.Assert.*;

@RunWith(Parameterized.class)
@UseParametersRunnerFactory(DatakernelRunnerFactory.class)
public class SharedKeyStorageTest {
	private static final StructuredCodec<SharedSimKey> SHARED_KEY_CODEC = REGISTRY.get(SharedSimKey.class);
	private static final String MY_SQL_PROPERTIES = "test_sharedKeys.properties";

	@Rule
	public TemporaryFolder temporaryFolder = new TemporaryFolder();

	@Parameter()
	public Function<Path, SharedKeyStorage> storageFn;

	private SharedKeyStorage storage;

	@Parameters(name = "{0}")
	public static Collection<Object[]> getParameters() {
		return Arrays.asList(
				new Object[]{(Function<Path, SharedKeyStorage>) path -> {
					try {
						return RocksDbSharedKeyStorage.create(RocksDB.open(path.toString()));
					} catch (RocksDBException e) {
						throw new AssertionError(e);
					}
				}
				},
				new Object[]{(Function<Path, SharedKeyStorage>) path -> {
					try {
						DataSource dataSource = dataSource(MY_SQL_PROPERTIES);
						return MySqlSharedKeyStorage.create(dataSource);
					} catch (IOException e) {
						System.out.println("WARNING: Failed to get properties from " + MY_SQL_PROPERTIES + " (" +
								e.getMessage() + "), using stub instead");
						return new InMemorySharedKeyStorage();
					}
				}
				}
		);
	}

	@Before
	public void before() throws IOException, SQLException {
		storage = storageFn.apply(temporaryFolder.newFolder().toPath());
		if (storage instanceof MySqlSharedKeyStorage) {
			((MySqlSharedKeyStorage) storage).initialize();
			((MySqlSharedKeyStorage) storage).truncateTables();
		}
	}

	@Test
	public void testStoreAndLoad() {
		PubKey receiver = KeyPair.generate().getPubKey();
		PrivKey sender = KeyPair.generate().getPrivKey();
		SimKey simKey = SimKey.generate();

		SharedSimKey sharedSimKey = SharedSimKey.of(simKey, receiver);
		SignedData<SharedSimKey> signedSharedKey = SignedData.sign(SHARED_KEY_CODEC, sharedSimKey, sender);

		await(storage.store(receiver, signedSharedKey));
		SignedData<SharedSimKey> fetchedKey = await(storage.load(receiver, sharedSimKey.getHash()));

		assertEquals(signedSharedKey, fetchedKey);
	}

	@Test
	public void testLoadEmpty() {
		assertNull(await(storage.load(KeyPair.generate().getPubKey(), Hash.sha1(new byte[0]))));
	}

	@Test
	public void testStoreMultipleSameKey() {
		PubKey receiver = KeyPair.generate().getPubKey();
		PrivKey sender = KeyPair.generate().getPrivKey();
		SimKey simKey1 = SimKey.generate();
		SimKey simKey2 = SimKey.generate();

		SharedSimKey sharedSimKey1 = SharedSimKey.of(simKey1, receiver);
		SignedData<SharedSimKey> signedSharedKey1 = SignedData.sign(SHARED_KEY_CODEC, sharedSimKey1, sender);

		SharedSimKey sharedSimKey2 = SharedSimKey.of(simKey2, receiver);
		SignedData<SharedSimKey> signedSharedKey2 = SignedData.sign(SHARED_KEY_CODEC, sharedSimKey2, sender);

		await(storage.store(receiver, signedSharedKey1));
		await(storage.store(receiver, signedSharedKey2));
		SignedData<SharedSimKey> fetchedKey1 = await(storage.load(receiver, sharedSimKey1.getHash()));
		SignedData<SharedSimKey> fetchedKey2 = await(storage.load(receiver, sharedSimKey2.getHash()));

		assertEquals(signedSharedKey1, fetchedKey1);
		assertEquals(signedSharedKey2, fetchedKey2);
	}

	@Test
	public void testStoreMultipleKeys() {
		PubKey receiver1 = KeyPair.generate().getPubKey();
		PubKey receiver2 = KeyPair.generate().getPubKey();
		PrivKey sender1 = KeyPair.generate().getPrivKey();
		PrivKey sender2 = KeyPair.generate().getPrivKey();
		SimKey simKey1 = SimKey.generate();
		SimKey simKey2 = SimKey.generate();

		SharedSimKey sharedSimKey1 = SharedSimKey.of(simKey1, receiver1);
		SignedData<SharedSimKey> signedSharedKey1 = SignedData.sign(SHARED_KEY_CODEC, sharedSimKey1, sender1);

		SharedSimKey sharedSimKey2 = SharedSimKey.of(simKey2, receiver2);
		SignedData<SharedSimKey> signedSharedKey2 = SignedData.sign(SHARED_KEY_CODEC, sharedSimKey2, sender2);

		await(storage.store(receiver1, signedSharedKey1));
		await(storage.store(receiver2, signedSharedKey2));
		SignedData<SharedSimKey> fetchedKey1 = await(storage.load(receiver1, sharedSimKey1.getHash()));
		SignedData<SharedSimKey> fetchedKey2 = await(storage.load(receiver2, sharedSimKey2.getHash()));

		assertEquals(signedSharedKey1, fetchedKey1);
		assertEquals(signedSharedKey2, fetchedKey2);
	}

	@Test
	public void testStoreOverride() {
		PubKey receiver = KeyPair.generate().getPubKey();
		PrivKey sender = KeyPair.generate().getPrivKey();
		SimKey simKey = SimKey.generate();

		SharedSimKey sharedSimKey = SharedSimKey.of(simKey, receiver);
		SignedData<SharedSimKey> signedSharedKey = SignedData.sign(SHARED_KEY_CODEC, sharedSimKey, sender);

		await(storage.store(receiver, signedSharedKey));

		SharedSimKey newSharedSimKey = SharedSimKey.of(SimKey.generate(), receiver);
		SignedData<SharedSimKey> newSignedKey = SignedData.sign(SHARED_KEY_CODEC, newSharedSimKey, sender);

		assertNotEquals(signedSharedKey, newSignedKey);

		await(storage.store(receiver, newSignedKey));
		SignedData<SharedSimKey> fetchedKey = await(storage.load(receiver, newSharedSimKey.getHash()));

		assertEquals(newSignedKey, fetchedKey);
	}

}
