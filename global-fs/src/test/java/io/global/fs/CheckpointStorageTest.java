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

import io.datakernel.eventloop.Eventloop;
import io.datakernel.remotefs.LocalFsClient;
import io.datakernel.test.rules.ByteBufRule;
import io.datakernel.test.rules.EventloopRule;
import io.global.common.KeyPair;
import io.global.common.SignedData;
import io.global.fs.api.GlobalFsCheckpoint;
import io.global.fs.local.RemoteFsCheckpointStorage;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.spongycastle.crypto.digests.SHA256Digest;

import java.io.IOException;
import java.nio.file.Path;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;

import static io.datakernel.async.TestUtils.await;
import static io.global.fs.util.BinaryDataFormats.REGISTRY;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertTrue;

public final class CheckpointStorageTest {

	@Rule
	public TemporaryFolder temporaryFolder = new TemporaryFolder();

	@ClassRule
	public static final EventloopRule eventloopRule = new EventloopRule();

	@ClassRule
	public static final ByteBufRule byteBufRule = new ByteBufRule();

	private RemoteFsCheckpointStorage storage;

	@Before
	public void setUp() throws IOException {
		Executor executor = Executors.newSingleThreadExecutor();
		Path path = temporaryFolder.newFolder().toPath();

		storage = new RemoteFsCheckpointStorage(LocalFsClient.create(Eventloop.getCurrentEventloop(), path).withRevisions());
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

		await(storage.store("test.txt", SignedData.sign(REGISTRY.get(GlobalFsCheckpoint.class), GlobalFsCheckpoint.of(filename, 567, 1, digest1, null), keys.getPrivKey())));
		await(storage.store("test.txt", SignedData.sign(REGISTRY.get(GlobalFsCheckpoint.class), GlobalFsCheckpoint.of(filename, 123, 1, digest2, null), keys.getPrivKey())));
		await(storage.store("test.txt", SignedData.sign(REGISTRY.get(GlobalFsCheckpoint.class), GlobalFsCheckpoint.of(filename, 321, 1, digest3, null), keys.getPrivKey())));

		long[] positions = await(storage.loadIndex("test.txt"));
		assertArrayEquals(new long[]{123, 321, 567}, positions);

		SignedData<GlobalFsCheckpoint> checkpoint1 = await(storage.load("test.txt", 321));
		assertTrue(checkpoint1.verify(keys.getPubKey()));

		SignedData<GlobalFsCheckpoint> checkpoint2 = await(storage.load("test.txt", 567));
		assertTrue(checkpoint2.verify(keys.getPubKey()));

		SignedData<GlobalFsCheckpoint> checkpoint3 = await(storage.load("test.txt", 123));
		assertTrue(checkpoint3.verify(keys.getPubKey()));
	}
}
