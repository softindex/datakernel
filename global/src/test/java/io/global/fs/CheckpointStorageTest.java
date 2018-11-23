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
import io.datakernel.eventloop.Eventloop;
import io.datakernel.remotefs.LocalFsClient;
import io.datakernel.stream.processor.DatakernelRunner;
import io.global.common.KeyPair;
import io.global.common.SignedData;
import io.global.fs.api.GlobalFsCheckpoint;
import io.global.fs.local.RemoteFsCheckpointStorage;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.spongycastle.crypto.digests.SHA256Digest;

import java.io.IOException;
import java.nio.file.Path;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static io.global.fs.util.BinaryDataFormats.REGISTRY;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertTrue;

@RunWith(DatakernelRunner.class)
public final class CheckpointStorageTest {

	@Rule
	public TemporaryFolder temporaryFolder = new TemporaryFolder();
	private RemoteFsCheckpointStorage storage;

	@Before
	public void setUp() throws IOException {
		ExecutorService executor = Executors.newSingleThreadExecutor();
		Path path = temporaryFolder.newFolder().toPath();

		storage = new RemoteFsCheckpointStorage(LocalFsClient.create(Eventloop.getCurrentEventloop(), executor, path));
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

		String filename = "test.txt";

		Promise.complete()
				.thenCompose($ -> storage.store("test.txt", SignedData.sign(REGISTRY.get(GlobalFsCheckpoint.class), GlobalFsCheckpoint.of(filename, 567, digest1), keys.getPrivKey())))
				.thenCompose($ -> storage.store("test.txt", SignedData.sign(REGISTRY.get(GlobalFsCheckpoint.class), GlobalFsCheckpoint.of(filename, 123, digest2), keys.getPrivKey())))
				.thenCompose($ -> storage.store("test.txt", SignedData.sign(REGISTRY.get(GlobalFsCheckpoint.class), GlobalFsCheckpoint.of(filename, 321, digest3), keys.getPrivKey())))
				.thenCompose($ -> storage.loadIndex("test.txt"))
				.whenResult(positions -> assertArrayEquals(new long[]{123, 321, 567}, positions))
				.thenCompose($ -> storage.load("test.txt", 321))
				.whenResult(checkpoint -> assertTrue(checkpoint.verify(keys.getPubKey())))
				.thenCompose($ -> storage.load("test.txt", 567))
				.whenResult(checkpoint -> assertTrue(checkpoint.verify(keys.getPubKey())))
				.thenCompose($ -> storage.load("test.txt", 123))
				.whenResult(checkpoint -> assertTrue(checkpoint.verify(keys.getPubKey())));
	}
}
