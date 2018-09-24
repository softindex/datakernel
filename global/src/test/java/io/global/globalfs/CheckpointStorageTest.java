package io.global.globalfs;

import io.datakernel.async.Stage;
import io.datakernel.eventloop.Eventloop;
import io.datakernel.remotefs.LocalFsClient;
import io.global.common.KeyPair;
import io.global.common.SignedData;
import io.global.globalfs.api.GlobalFsCheckpoint;
import io.global.globalfs.server.CheckpointStorageFs;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.spongycastle.crypto.digests.SHA256Digest;

import java.io.IOException;
import java.nio.file.Path;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static io.datakernel.eventloop.FatalErrorHandlers.rethrowOnAnyError;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertTrue;

public class CheckpointStorageTest {

	@Rule
	public TemporaryFolder temporaryFolder = new TemporaryFolder();
	private CheckpointStorageFs storage;
	private Eventloop eventloop;

	@Before
	public void setUp() throws IOException {
		ExecutorService executor = Executors.newSingleThreadExecutor();
		Path path = temporaryFolder.newFolder().toPath();

		eventloop = Eventloop.create().withCurrentThread().withFatalErrorHandler(rethrowOnAnyError());
		storage = new CheckpointStorageFs(LocalFsClient.create(eventloop, executor, path));
	}

	@Test
	public void test() {
		KeyPair keys = KeyPair.generate();

		SHA256Digest digest1 = new SHA256Digest();
		for (int i = 0; i < 567; i++) {
			digest1.update((byte) (i % 128));
		}

		SHA256Digest digest2 = new SHA256Digest();
		for (int i = 0; i < 123; i++) {
			digest2.update((byte) (i % 128));
		}

		SHA256Digest digest3 = new SHA256Digest();
		for (int i = 0; i < 321; i++) {
			digest3.update((byte) (i % 128));
		}

		Stage.complete()
				.thenCompose($ -> storage.saveCheckpoint("test.txt", SignedData.sign(GlobalFsCheckpoint.of(567, digest1), keys.getPrivKey())))
				.thenCompose($ -> storage.saveCheckpoint("test.txt", SignedData.sign(GlobalFsCheckpoint.of(123, digest2), keys.getPrivKey())))
				.thenCompose($ -> storage.saveCheckpoint("test.txt", SignedData.sign(GlobalFsCheckpoint.of(321, digest3), keys.getPrivKey())))
				.thenCompose($ -> storage.getCheckpoints("test.txt"))
				.whenResult(positions -> assertArrayEquals(new long[]{123, 321, 567}, positions))
				.thenCompose($ -> storage.loadCheckpoint("test.txt", 321))
				.whenResult(checkpoint -> assertTrue(checkpoint.verify(keys.getPubKey())))
				.thenCompose($ -> storage.loadCheckpoint("test.txt", 567))
				.whenResult(checkpoint -> assertTrue(checkpoint.verify(keys.getPubKey())))
				.thenCompose($ -> storage.loadCheckpoint("test.txt", 123))
				.whenResult(checkpoint -> assertTrue(checkpoint.verify(keys.getPubKey())));

		eventloop.run();
	}
}
