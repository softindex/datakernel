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

import io.datakernel.async.Promise;
import io.datakernel.crdt.local.FsCrdtClient;
import io.datakernel.eventloop.Eventloop;
import io.datakernel.remotefs.LocalFsClient;
import io.datakernel.stream.StreamSupplier;
import io.datakernel.stream.processor.DatakernelRunner;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.Executors;
import java.util.stream.Stream;

import static io.datakernel.serializer.asm.BufferSerializers.*;
import static io.datakernel.test.TestUtils.assertComplete;
import static io.datakernel.util.CollectionUtils.set;

@RunWith(DatakernelRunner.class)
public final class TestCrdtLocalFileConsolidation {
	private LocalFsClient fsClient;

	@Rule
	public TemporaryFolder temporaryFolder = new TemporaryFolder();

	@Before
	public void setup() throws IOException {
		fsClient = LocalFsClient.create(Eventloop.getCurrentEventloop(), Executors.newSingleThreadExecutor(), temporaryFolder.newFolder().toPath());
	}

	private Set<Integer> union(Set<Integer> first, Set<Integer> second) {
		Set<Integer> res = new HashSet<>(Math.max((int) ((first.size() + second.size()) / .75f) + 1, 16));
		first.addAll(second);
		return res;
	}

	@Test
	public void test() {
		FsCrdtClient<String, Set<Integer>> client = FsCrdtClient.create(Eventloop.getCurrentEventloop(), fsClient, this::union, JAVA_UTF8_SERIALIZER, ofSet(INT_SERIALIZER));

		Promise.complete()
				.thenCompose($ -> StreamSupplier.ofStream(Stream.of(
						new CrdtData<>("1_test_1", set(1, 2, 3)),
						new CrdtData<>("1_test_2", set(2, 3, 7)),
						new CrdtData<>("1_test_3", set(78, 2, 3)),
						new CrdtData<>("12_test_1", set(123, 124, 125)),
						new CrdtData<>("12_test_2", set(12))
				).sorted())
						.streamTo(client.uploader()))
				.thenCompose($ -> StreamSupplier.ofStream(Stream.of(
						new CrdtData<>("2_test_1", set(1, 2, 3)),
						new CrdtData<>("2_test_2", set(2, 3, 4)),
						new CrdtData<>("2_test_3", set(0, 1, 2)),
						new CrdtData<>("12_test_1", set(123, 542, 125, 2)),
						new CrdtData<>("12_test_2", set(12, 13))
				).sorted())
						.streamTo(client.uploader()))
				.thenCompose($ -> fsClient.list().whenResult(System.out::println))
				.thenCompose($ -> client.consolidate())
				.thenCompose($ -> fsClient.list().whenResult(System.out::println))
				.whenComplete(assertComplete());
	}
}
