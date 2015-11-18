/*
 * Copyright (C) 2015 SoftIndex LLC.
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

package io.datakernel.hashfs;

import com.google.common.base.Charsets;
import io.datakernel.async.CompletionCallback;
import io.datakernel.async.ResultCallback;
import io.datakernel.bytebuf.ByteBufPool;
import io.datakernel.eventloop.NioEventloop;
import io.datakernel.stream.file.StreamFileReader;
import io.datakernel.stream.file.StreamFileWriter;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.ExecutorService;

import static io.datakernel.async.AsyncCallbacks.ignoreCompletionCallback;
import static io.datakernel.bytebuf.ByteBufPool.getPoolItemsString;
import static java.util.concurrent.Executors.newCachedThreadPool;
import static org.junit.Assert.*;

public class TestFileSystem {
	@Rule
	public final TemporaryFolder tmpFolder = new TemporaryFolder();

	private NioEventloop eventloop = new NioEventloop();
	private ExecutorService executor = newCachedThreadPool();
	private Path storage;
	private Path client;

	@Before
	public void setup() throws IOException {
		storage = Paths.get(tmpFolder.newFolder("storage").toURI());
		client = Paths.get(tmpFolder.newFolder("client").toURI());

		Files.createDirectories(storage);
		Files.createDirectories(client);

		Path f = client.resolve("f.txt");
		Files.write(f, ("some text1\n\nmore text1\t\n\n\r").getBytes(Charsets.UTF_8));

		Path c = client.resolve("c.txt");
		Files.write(c, ("some text2\n\nmore text2\t\n\n\r").getBytes(Charsets.UTF_8));

		Files.createDirectories(storage.resolve("1"));
		Files.createDirectories(storage.resolve("2/3"));
		Files.createDirectories(storage.resolve("2/b"));

		Path a1 = storage.resolve("1/a.txt");
		Files.write(a1, ("1\n2\n3\n4\n5\n6\n").getBytes(Charsets.UTF_8));

		Path b = storage.resolve("1/b.txt");
		Files.write(b, ("7\n8\n9\n10\n11\n12\n").getBytes(Charsets.UTF_8));

		Path a2 = storage.resolve("2/3/a.txt");
		Files.write(a2, ("6\n5\n4\n3\n2\n1\n").getBytes(Charsets.UTF_8));

		Path d = storage.resolve("2/b/d.txt");
		StringBuilder sb = new StringBuilder();
		for (int i = 0; i < 1_000_000; i++) {
			sb.append(i).append("\n");
		}
		Files.write(d, sb.toString().getBytes(Charsets.UTF_8));

		Path e = storage.resolve("2/b/e.txt");
		Files.createFile(e);
	}

	@Test
	public void testUpload() throws IOException {
		FileSystemImpl fs = FileSystemImpl.buildInstance(eventloop, executor, storage).build();
		fs.start(ignoreCompletionCallback());

		StreamFileReader producer = StreamFileReader.readFileFully(eventloop, executor, 1024, client.resolve("c.txt"));
		fs.saveToTmp("1/c.txt", producer, ignoreCompletionCallback());

		eventloop.run();

		assertTrue(com.google.common.io.Files.equal(client.resolve("c.txt").toFile(), storage.resolve("tmp/1/c.txt.partial").toFile()));
		assertEquals(getPoolItemsString(), ByteBufPool.getCreatedItems(), ByteBufPool.getPoolItems());

		fs.commitTmp("1/c.txt", ignoreCompletionCallback());
		eventloop.run();
		executor.shutdown();

		assertFalse(Files.exists(storage.resolve("tmp/1/c.txt.partial")));
		assertTrue(com.google.common.io.Files.equal(client.resolve("c.txt").toFile(), storage.resolve("1/c.txt").toFile()));
		assertEquals(getPoolItemsString(), ByteBufPool.getCreatedItems(), ByteBufPool.getPoolItems());
	}

	@Test
	public void testUploadFailed() throws IOException {
		FileSystemImpl fs = FileSystemImpl.buildInstance(eventloop, executor, storage).build();
		fs.start(ignoreCompletionCallback());

		StreamFileReader producer = StreamFileReader.readFileFully(eventloop, executor, 1024, client.resolve("c.txt"));
		fs.saveToTmp("1/c.txt", producer, ignoreCompletionCallback());

		eventloop.run();

		assertTrue(com.google.common.io.Files.equal(client.resolve("c.txt").toFile(), storage.resolve("tmp/1/c.txt.partial").toFile()));
		assertEquals(getPoolItemsString(), ByteBufPool.getCreatedItems(), ByteBufPool.getPoolItems());

		// Failed on client side --> decide to cancel upload
		fs.deleteTmp("1/c.txt", ignoreCompletionCallback());

		assertFalse(Files.exists(storage.resolve("tmp/1/c.txt.partial")));
		assertTrue(Files.exists(storage.resolve("tmp")));
		assertEquals(getPoolItemsString(), ByteBufPool.getCreatedItems(), ByteBufPool.getPoolItems());
	}

	@Test
	public void testGet() throws IOException {
		FileSystemImpl fs = FileSystemImpl.buildInstance(eventloop, executor, storage).build();

		fs.start(ignoreCompletionCallback());

		StreamFileWriter consumer = StreamFileWriter.createFile(eventloop, executor, client.resolve("d.txt"));

		fs.get("2/b/d.txt").streamTo(consumer);

		eventloop.run();
		executor.shutdown();
		assertTrue(com.google.common.io.Files.equal(client.resolve("d.txt").toFile(), storage.resolve("2/b/d.txt").toFile()));
		assertEquals(getPoolItemsString(), ByteBufPool.getCreatedItems(), ByteBufPool.getPoolItems());
	}

	@Test
	public void testGetFailed() throws Exception {
		FileSystemImpl fs = FileSystemImpl.buildInstance(eventloop, executor, storage).build();

		fs.start(ignoreCompletionCallback());
		StreamFileWriter consumer = StreamFileWriter.createFile(eventloop, executor, client.resolve("no_file.txt"));

		consumer.setFlushCallback(new CompletionCallback() {
			@Override
			public void onComplete() {
				fail("Should not get there");
			}

			@Override
			public void onException(Exception e) {
				assertTrue(e.getClass() == NoSuchFileException.class);
			}
		});
		fs.get("2/b/no_file.txt").streamTo(consumer);

		eventloop.run();
		executor.shutdown();

		assertEquals(getPoolItemsString(), ByteBufPool.getCreatedItems(), ByteBufPool.getPoolItems());
	}

	@Test
	public void testDeleteFile() throws IOException {
		FileSystemImpl fs = FileSystemImpl.buildInstance(eventloop, executor, storage).build();

		fs.start(ignoreCompletionCallback());
		assertTrue(Files.exists(storage.resolve("2/3/a.txt")));
		fs.delete("2/3/a.txt", ignoreCompletionCallback());
		assertFalse(Files.exists(storage.resolve("2/3/a.txt")));
		assertFalse(Files.exists(storage.resolve("2/3")));
		assertEquals(getPoolItemsString(), ByteBufPool.getCreatedItems(), ByteBufPool.getPoolItems());
	}

	@Test
	public void testDeleteFailed() throws IOException {
		FileSystemImpl fs = FileSystemImpl.buildInstance(eventloop, executor, storage).build();

		fs.start(ignoreCompletionCallback());
		fs.delete("2/3/z.txt", new CompletionCallback() {
			@Override
			public void onComplete() {
				fail("Should not end here");
			}

			@Override
			public void onException(Exception e) {
				assertTrue(e.getClass() == NoSuchFileException.class);
			}
		});
		assertEquals(getPoolItemsString(), ByteBufPool.getCreatedItems(), ByteBufPool.getPoolItems());
	}

	@Test
	public void testListFiles() throws IOException {
		FileSystemImpl fs = FileSystemImpl.buildInstance(eventloop, executor, storage).build();

		fs.start(ignoreCompletionCallback());
		final Set<String> expected = new HashSet<>();
		expected.addAll(Arrays.asList("1/a.txt", "1/b.txt", "2/3/a.txt", "2/b/d.txt", "2/b/e.txt"));
		fs.list(new ResultCallback<Set<String>>() {
			@Override
			public void onResult(Set<String> result) {
				assertEquals(expected, result);
			}

			@Override
			public void onException(Exception exception) {
				fail("Should not get here");
			}
		});

		assertEquals(getPoolItemsString(), ByteBufPool.getCreatedItems(), ByteBufPool.getPoolItems());
	}
}
