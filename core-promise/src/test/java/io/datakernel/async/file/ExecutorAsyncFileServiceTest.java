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

package io.datakernel.async.file;

import io.datakernel.promise.Promises;
import io.datakernel.test.rules.EventloopRule;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.concurrent.Executors;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static io.datakernel.common.collection.CollectionUtils.set;
import static io.datakernel.promise.TestUtils.await;
import static java.nio.file.StandardOpenOption.READ;
import static java.nio.file.StandardOpenOption.WRITE;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

public final class ExecutorAsyncFileServiceTest {
	@Rule
	public TemporaryFolder temporaryFolder = new TemporaryFolder();
	@ClassRule
	public static final EventloopRule eventloopRule = new EventloopRule();

	private ExecutorAsyncFileService service = new ExecutorAsyncFileService(Executors.newCachedThreadPool());

	static {
		System.setProperty("AsyncFileService.aio", "false");
	}

	@Test
	public void testRead() throws IOException {
		Path srcPath = Paths.get("test_data/test.txt");
		FileChannel channel = FileChannel.open(srcPath, set(READ));

		byte[] result = new byte[20];
		await(Promises.all(IntStream.range(0, 100)
				.mapToObj(i -> service.read(channel, 0, result, 0, result.length)
						.whenComplete((res, e) -> {
							if (e != null) {
								e.printStackTrace();
								fail();
							}

							try {
								assertEquals(Files.readAllBytes(srcPath).length, res.intValue());
							} catch (IOException e1) {
								e1.printStackTrace();
							}
						})).collect(Collectors.toList())));
	}

	@Test
	public void testWrite() throws IOException {
		Path srcPath = Paths.get("test_data/test.txt");
		FileChannel channel = FileChannel.open(srcPath, set(READ, WRITE));
		byte[] array = "Hello world!!!!!".getBytes();

		await(Promises.all(IntStream.range(0, 1000)
				.mapToObj($ -> service.write(channel, 0, array, 0, array.length)
						.whenComplete((res, e) -> {
							if (e != null) {
								e.printStackTrace();
								fail();
							}
							assertEquals(res.intValue(), array.length);
						}))));
	}
}
