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
import java.util.Arrays;
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

		Stage.complete()
				.thenCompose($ -> storage.saveCheckpoint("test.txt", SignedData.sign(GlobalFsCheckpoint.of(567, new SHA256Digest()), keys.getPrivKey())))
				.thenCompose($ -> storage.saveCheckpoint("test.txt", SignedData.sign(GlobalFsCheckpoint.of(123, new SHA256Digest()), keys.getPrivKey())))
				.thenCompose($ -> storage.saveCheckpoint("test.txt", SignedData.sign(GlobalFsCheckpoint.of(321, new SHA256Digest()), keys.getPrivKey())))
				.thenCompose($ -> storage.getCheckpoints("test.txt"))
				.whenResult(positions -> System.out.println(Arrays.toString(positions)))
				.whenResult(positions -> assertArrayEquals(new long[]{123, 321, 567}, positions))
				.thenCompose($ -> storage.loadCheckpoint("test.txt", 123))
				.whenResult(checkpoint -> assertTrue(checkpoint.verify(keys.getPubKey())));

		eventloop.run();
	}

	@Test
	public void what() throws IOException {
		GlobalFsCheckpoint.ofBytes(GlobalFsCheckpoint.of(123, new SHA256Digest()).toBytes());
	}
}
