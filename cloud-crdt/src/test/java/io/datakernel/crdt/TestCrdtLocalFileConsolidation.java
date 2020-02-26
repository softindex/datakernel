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

package io.datakernel.crdt;

import io.datakernel.crdt.CrdtData.CrdtDataSerializer;
import io.datakernel.crdt.local.CrdtClientFs;
import io.datakernel.crdt.primitives.GSet;
import io.datakernel.datastream.StreamConsumer;
import io.datakernel.datastream.StreamSupplier;
import io.datakernel.eventloop.Eventloop;
import io.datakernel.remotefs.LocalFsClient;
import io.datakernel.test.rules.ByteBufRule;
import io.datakernel.test.rules.EventloopRule;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.IOException;
import java.util.stream.Stream;

import static io.datakernel.promise.TestUtils.await;
import static io.datakernel.serializer.BinarySerializers.INT_SERIALIZER;
import static io.datakernel.serializer.BinarySerializers.UTF8_SERIALIZER;

public final class TestCrdtLocalFileConsolidation {
	private LocalFsClient fsClient;

	@ClassRule
	public static final EventloopRule eventloopRule = new EventloopRule();

	@ClassRule
	public static final ByteBufRule byteBufRule = new ByteBufRule();

	@Rule
	public TemporaryFolder temporaryFolder = new TemporaryFolder();

	@Before
	public void setup() throws IOException {
		fsClient = LocalFsClient.create(Eventloop.getCurrentEventloop(), temporaryFolder.newFolder().toPath());
	}

	@Test
	public void test() {
		CrdtDataSerializer<String, GSet<Integer>> serializer = new CrdtDataSerializer<>(UTF8_SERIALIZER, new GSet.Serializer<>(INT_SERIALIZER));
		CrdtClientFs<String, GSet<Integer>> client = CrdtClientFs.create(Eventloop.getCurrentEventloop(), fsClient, serializer);

		await(StreamSupplier.ofStream(Stream.of(
				new CrdtData<>("1_test_1", GSet.of(1, 2, 3)),
				new CrdtData<>("1_test_2", GSet.of(2, 3, 7)),
				new CrdtData<>("1_test_3", GSet.of(78, 2, 3)),
				new CrdtData<>("12_test_1", GSet.of(123, 124, 125)),
				new CrdtData<>("12_test_2", GSet.of(12))).sorted())
				.streamTo(StreamConsumer.ofPromise(client.upload())));
		await(StreamSupplier.ofStream(Stream.of(
				new CrdtData<>("2_test_1", GSet.of(1, 2, 3)),
				new CrdtData<>("2_test_2", GSet.of(2, 3, 4)),
				new CrdtData<>("2_test_3", GSet.of(0, 1, 2)),
				new CrdtData<>("12_test_1", GSet.of(123, 542, 125, 2)),
				new CrdtData<>("12_test_2", GSet.of(12, 13))).sorted())
				.streamTo(StreamConsumer.ofPromise(client.upload())));

		System.out.println(await(fsClient.list("**")));
		await(client.consolidate());
		System.out.println(await(fsClient.list("**")));
	}
}
