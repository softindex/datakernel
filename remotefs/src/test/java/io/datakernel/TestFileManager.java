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

package io.datakernel;

import com.google.common.base.Charsets;
import io.datakernel.async.*;
import io.datakernel.eventloop.Eventloop;
import io.datakernel.remotefs.FileManager;
import io.datakernel.stream.file.StreamFileReader;
import io.datakernel.stream.file.StreamFileWriter;
import org.hamcrest.BaseMatcher;
import org.hamcrest.Description;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.rules.TemporaryFolder;

import java.io.IOException;
import java.nio.file.*;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;

import static io.datakernel.bytebuf.ByteBufPool.*;
import static io.datakernel.eventloop.FatalErrorHandlers.rethrowOnAnyError;
import static io.datakernel.file.AsyncFile.open;
import static io.datakernel.stream.file.StreamFileReader.readFileFully;
import static java.nio.file.Files.*;
import static java.util.Arrays.asList;
import static java.util.concurrent.Executors.newCachedThreadPool;
import static org.junit.Assert.*;

public class TestFileManager {
	private static final OpenOption[] READ_OPTIONS = new OpenOption[]{StandardOpenOption.READ};
	private static final OpenOption[] CREATE_OPTIONS = StreamFileWriter.CREATE_OPTIONS;

	@Rule
	public final TemporaryFolder tmpFolder = new TemporaryFolder();

	@Rule
	public final ExpectedException thrown = ExpectedException.none();

	private Eventloop eventloop = Eventloop.create().withFatalErrorHandler(rethrowOnAnyError());
	private ExecutorService executor = newCachedThreadPool();
	private Path storage;
	private Path client;

	private static final int bufferSize = 2;

	@Before
	public void setup() throws IOException {
		storage = Paths.get(tmpFolder.newFolder("storage").toURI());
		client = Paths.get(tmpFolder.newFolder("client").toURI());

		createDirectories(storage);
		createDirectories(client);

		Path f = client.resolve("f.txt");
		write(f, ("some text1\n\nmore text1\t\n\n\r").getBytes(Charsets.UTF_8));

		Path c = client.resolve("c.txt");
		write(c, ("some text2\n\nmore text2\t\n\n\r").getBytes(Charsets.UTF_8));

		createDirectories(storage.resolve("1"));
		createDirectories(storage.resolve("2/3"));
		createDirectories(storage.resolve("2/b"));

		Path a1 = storage.resolve("1/a.txt");
		write(a1, ("1\n2\n3\n4\n5\n6\n").getBytes(Charsets.UTF_8));

		Path b = storage.resolve("1/b.txt");
		write(b, ("7\n8\n9\n10\n11\n12\n").getBytes(Charsets.UTF_8));

		Path a2 = storage.resolve("2/3/a.txt");
		write(a2, ("6\n5\n4\n3\n2\n1\n").getBytes(Charsets.UTF_8));

		Path d = storage.resolve("2/b/d.txt");
		StringBuilder sb = new StringBuilder();
		for (int i = 0; i < 1_000_000; i++) {
			sb.append(i).append("\n");
		}
		write(d, sb.toString().getBytes(Charsets.UTF_8));

		Path e = storage.resolve("2/b/e.txt");
		createFile(e);
	}

	@Test
	public void testDoUpload() throws IOException {
		FileManager fs = FileManager.create(eventloop, executor, storage);
		final Path inputFile = client.resolve("c.txt");

		fs.save("1/c.txt", new ForwardingResultCallback<StreamFileWriter>(IgnoreCompletionCallback.create()) {
			@Override
			public void onResult(StreamFileWriter writer) {
				try {
					readFileFully(eventloop,
							open(eventloop, executor, inputFile, READ_OPTIONS), bufferSize)
							.streamTo(writer);
				} catch (IOException e) {
					this.setException(e);
				}
			}
		});
		eventloop.run();
		executor.shutdown();

		assertArrayEquals(readAllBytes(inputFile), readAllBytes(storage.resolve("1/c.txt")));
		assertEquals(getPoolItemsString(), getCreatedItems(), getPoolItems());
	}

	@Test
	public void testDoDownload() throws IOException {
		FileManager fs = FileManager.create(eventloop, executor, storage);
		final Path outputFile = client.resolve("d.txt");

		fs.get("2/b/d.txt", 0, new ForwardingResultCallback<StreamFileReader>(IgnoreResultCallback.create()) {
			@Override
			public void onResult(StreamFileReader reader) {
				try {
					reader.streamTo(
							StreamFileWriter.create(eventloop, open(
									eventloop, executor, outputFile, CREATE_OPTIONS)));
				} catch (IOException e) {
					this.setException(e);
				}
			}
		});
		eventloop.run();
		executor.shutdown();

		assertArrayEquals(readAllBytes(storage.resolve("2/b/d.txt")), readAllBytes(outputFile));
		assertEquals(getPoolItemsString(), getCreatedItems(), getPoolItems());
	}

	@Test
	public void testDoDownloadFailed() throws Exception {
		FileManager fs = FileManager.create(eventloop, executor, storage);
		ResultCallbackFuture<StreamFileReader> callbackFuture = ResultCallbackFuture.create();

		fs.get("no_file.txt", 0, callbackFuture);
		eventloop.run();
		executor.shutdown();

		thrown.expect(ExecutionException.class);
		thrown.expectCause(new BaseMatcher<Throwable>() {
			@Override
			public boolean matches(Object item) {
				return item instanceof NoSuchFileException;
			}

			@Override
			public void describeTo(Description description) {
				// empty
			}
		});
		callbackFuture.get();
		assertEquals(getPoolItemsString(), getCreatedItems(), getPoolItems());
	}

	@Test
	public void testDeleteFile() {
		FileManager fs = FileManager.create(eventloop, executor, storage);
		assertTrue(exists(storage.resolve("2/3/a.txt")));

		fs.delete("2/3/a.txt", IgnoreCompletionCallback.create());
		eventloop.run();
		executor.shutdown();

		assertFalse(exists(storage.resolve("2/3/a.txt")));
		assertEquals(getPoolItemsString(), getCreatedItems(), getPoolItems());
	}

	@Test
	public void testDeleteFailed() throws Exception {
		FileManager fs = FileManager.create(eventloop, executor, storage);
		CompletionCallbackFuture callbackFuture = CompletionCallbackFuture.create();

		fs.delete("no_file.txt", callbackFuture);
		eventloop.run();
		executor.shutdown();

		thrown.expect(ExecutionException.class);
		thrown.expectCause(new BaseMatcher<Throwable>() {
			@Override
			public boolean matches(Object item) {
				return item instanceof NoSuchFileException;
			}

			@Override
			public void describeTo(Description description) {
				// empty
			}
		});
		callbackFuture.get();

		assertEquals(getPoolItemsString(), getCreatedItems(), getPoolItems());
	}

	@Test
	public void testListFiles() throws Exception {
		FileManager fs = FileManager.create(eventloop, executor, storage);
		List<String> expected = asList("1/a.txt", "1/b.txt", "2/3/a.txt", "2/b/d.txt", "2/b/e.txt");
		List<String> actual;

		ResultCallbackFuture<List<String>> callbackFuture = ResultCallbackFuture.create();

		fs.scan(callbackFuture);
		eventloop.run();
		executor.shutdown();
		actual = callbackFuture.get();
		Collections.sort(expected);
		Collections.sort(actual);

		assertEquals(expected, actual);
		assertEquals(getPoolItemsString(), getCreatedItems(), getPoolItems());
	}
}
