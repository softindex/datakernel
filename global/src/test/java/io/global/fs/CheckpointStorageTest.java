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

import io.datakernel.async.Stage;
import io.datakernel.eventloop.Eventloop;
import io.datakernel.remotefs.LocalFsClient;
import io.global.common.CryptoUtils;
import io.global.common.KeyPair;
import io.global.common.SignedData;
import io.global.fs.api.GlobalFsCheckpoint;
import io.global.fs.local.RemoteFsCheckpointStorage;
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
import static java.nio.charset.StandardCharsets.UTF_8;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertTrue;

public class CheckpointStorageTest {

	@Rule
	public TemporaryFolder temporaryFolder = new TemporaryFolder();
	private RemoteFsCheckpointStorage storage;
	private Eventloop eventloop;

	@Before
	public void setUp() throws IOException {
		ExecutorService executor = Executors.newSingleThreadExecutor();
		Path path = temporaryFolder.newFolder().toPath();

		eventloop = Eventloop.create().withCurrentThread().withFatalErrorHandler(rethrowOnAnyError());
		storage = new RemoteFsCheckpointStorage(LocalFsClient.create(eventloop, executor, path));
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

		byte[] filenameHash = CryptoUtils.sha256("test.txt".getBytes(UTF_8));

		Stage.complete()
				.thenCompose($ -> storage.saveCheckpoint("test.txt", SignedData.sign(GlobalFsCheckpoint.of(567, digest1, filenameHash), keys.getPrivKey())))
				.thenCompose($ -> storage.saveCheckpoint("test.txt", SignedData.sign(GlobalFsCheckpoint.of(123, digest2, filenameHash), keys.getPrivKey())))
				.thenCompose($ -> storage.saveCheckpoint("test.txt", SignedData.sign(GlobalFsCheckpoint.of(321, digest3, filenameHash), keys.getPrivKey())))
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
