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

package io.datakernel.remotefs;

import io.datakernel.bytebuf.ByteBuf;
import io.datakernel.bytebuf.ByteBufQueue;
import io.datakernel.common.MemSize;
import io.datakernel.csp.ChannelConsumer;
import io.datakernel.csp.ChannelSupplier;
import io.datakernel.csp.file.ChannelFileReader;
import io.datakernel.csp.file.ChannelFileWriter;
import io.datakernel.eventloop.Eventloop;
import io.datakernel.promise.Promise;
import io.datakernel.test.rules.ByteBufRule;
import io.datakernel.test.rules.EventloopRule;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadLocalRandom;

import static io.datakernel.common.collection.CollectionUtils.set;
import static io.datakernel.promise.TestUtils.await;
import static io.datakernel.promise.TestUtils.awaitException;
import static io.datakernel.remotefs.FsClient.FILE_EXISTS;
import static io.datakernel.remotefs.FsClient.FILE_NOT_FOUND;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.nio.file.StandardOpenOption.CREATE;
import static java.nio.file.StandardOpenOption.TRUNCATE_EXISTING;
import static java.util.concurrent.Executors.newCachedThreadPool;
import static java.util.stream.Collectors.toSet;
import static org.junit.Assert.*;

public final class TestLocalFsClient {
	private static final MemSize BUFFER_SIZE = MemSize.of(2);

	@ClassRule
	public static final EventloopRule eventloopRule = new EventloopRule();

	@ClassRule
	public static final ByteBufRule byteBufRule = new ByteBufRule();

	@Rule
	public final TemporaryFolder tmpFolder = new TemporaryFolder();

	private Path storagePath;
	private Path clientPath;

	private LocalFsClient client;

	@Before
	public void setup() throws IOException {
		storagePath = tmpFolder.newFolder("storage").toPath();
		clientPath = tmpFolder.newFolder("client").toPath();

		Files.createDirectories(storagePath);
		Files.createDirectories(clientPath);

		Path f = clientPath.resolve("f.txt");
		Files.write(f, "some text1\n\nmore text1\t\n\n\r".getBytes(UTF_8), CREATE, TRUNCATE_EXISTING);

		Path c = clientPath.resolve("c.txt");
		Files.write(c, "some text2\n\nmore text2\t\n\n\r".getBytes(UTF_8), CREATE, TRUNCATE_EXISTING);

		Files.createDirectories(storagePath.resolve("1"));
		Files.createDirectories(storagePath.resolve("2/3"));
		Files.createDirectories(storagePath.resolve("2/b"));

		Path a1 = storagePath.resolve("1/a.txt");
		Files.write(a1, "1\n2\n3\n4\n5\n6\n".getBytes(UTF_8), CREATE, TRUNCATE_EXISTING);

		Path b = storagePath.resolve("1/b.txt");
		Files.write(b, "7\n8\n9\n10\n11\n12\n".getBytes(UTF_8), CREATE, TRUNCATE_EXISTING);

		Path a2 = storagePath.resolve("2/3/a.txt");
		Files.write(a2, "6\n5\n4\n3\n2\n1\n".getBytes(UTF_8), CREATE, TRUNCATE_EXISTING);

		Path d = storagePath.resolve("2/b/d.txt");
		StringBuilder sb = new StringBuilder();
		for (int i = 0; i < 1_000_000; i++) {
			sb.append(i).append("\n");
		}
		Files.write(d, sb.toString().getBytes(UTF_8));

		Path e = storagePath.resolve("2/b/e.txt");
		try {
			Files.createFile(e);
		} catch (IOException ignored) {
		}

		client = LocalFsClient.create(Eventloop.getCurrentEventloop(), storagePath);
	}

	@Test
	public void testDoUpload() throws IOException {
		Path path = clientPath.resolve("c.txt");

		await(client.upload("1/c.txt")
				.then(consumer -> ChannelFileReader.open(newCachedThreadPool(), path)
						.then(file -> file.withBufferSize(BUFFER_SIZE).streamTo(consumer))));

		assertArrayEquals(Files.readAllBytes(path), Files.readAllBytes(storagePath.resolve("1/c.txt")));
	}

	@Test
	public void testConcurrentUpload() throws IOException {
		String file = "concurrent.txt";
		Files.write(storagePath.resolve(file), "Concurrent data - 1\nConcurr".getBytes());

		await(
				delayed(Arrays.asList(
						ByteBuf.wrapForReading("oncurrent data - 1\n".getBytes()),
						ByteBuf.wrapForReading("Concurrent data - 2\n".getBytes()),
						ByteBuf.wrapForReading("Concurrent data - 3\n".getBytes()),
						ByteBuf.wrapForReading("Concurrent data - 4\n".getBytes()),
						ByteBuf.wrapForReading("Concurrent data - 5\n".getBytes()),
						ByteBuf.wrapForReading("Concurrent data - 6\n".getBytes()),
						ByteBuf.wrapForReading("Concurrent data - 7\n".getBytes()),
						ByteBuf.wrapForReading("Concurrent data - 8\n".getBytes()),
						ByteBuf.wrapForReading("Concurrent data - 9\n".getBytes())))
						.streamTo(ChannelConsumer.ofPromise(client.upload(file, 1))),

				delayed(Arrays.asList(
						ByteBuf.wrapForReading(" data - 1\n".getBytes()),
						ByteBuf.wrapForReading("Concurrent data - 2\n".getBytes()),
						ByteBuf.wrapForReading("Concurrent data - 3\n".getBytes()),
						ByteBuf.wrapForReading("Concurrent data - 4\n".getBytes()),
						ByteBuf.wrapForReading("Concurrent data - 5\n".getBytes()),
						ByteBuf.wrapForReading("Concurrent data - 6\n".getBytes()),
						ByteBuf.wrapForReading("Concurrent data - 7\n".getBytes()),
						ByteBuf.wrapForReading("Concurrent data - 8\n".getBytes()),
						ByteBuf.wrapForReading("Concurrent data - 9\n".getBytes()),
						ByteBuf.wrapForReading("Concurrent data #2\n".getBytes()),
						ByteBuf.wrapForReading("Concurrent data #2\n".getBytes()),
						ByteBuf.wrapForReading("Concurrent data #2\n".getBytes()),
						ByteBuf.wrapForReading("Concurrent data #2\n".getBytes())))
						.streamTo(ChannelConsumer.ofPromise(client.upload(file, 10))),

				delayed(Arrays.asList(
						ByteBuf.wrapForReading(" - 1\n".getBytes()),
						ByteBuf.wrapForReading("Concurrent data - 2\n".getBytes()),
						ByteBuf.wrapForReading("Concurrent data - 3\n".getBytes()),
						ByteBuf.wrapForReading("Concurrent data - 4\n".getBytes()),
						ByteBuf.wrapForReading("Concurrent data - 5\n".getBytes()),
						ByteBuf.wrapForReading("Concurrent data - 6\n".getBytes()),
						ByteBuf.wrapForReading("Concurrent data - 7\n".getBytes()),
						ByteBuf.wrapForReading("Concurrent data - 8\n".getBytes()),
						ByteBuf.wrapForReading("Concurrent data - 9\n".getBytes()),
						ByteBuf.wrapForReading("Concurrent data #2\n".getBytes()),
						ByteBuf.wrapForReading("Concurrent data #2\n".getBytes()),
						ByteBuf.wrapForReading("Concurrent data #2\n".getBytes()),
						ByteBuf.wrapForReading("Concurrent data #2\n".getBytes())))
						.streamTo(ChannelConsumer.ofPromise(client.upload(file, 15))),

				delayed(Arrays.asList(
						ByteBuf.wrapForReading("urrent data - 2\n".getBytes()),
						ByteBuf.wrapForReading("Concurrent data - 3\n".getBytes()),
						ByteBuf.wrapForReading("Concurrent data - 4\n".getBytes()),
						ByteBuf.wrapForReading("Concurrent data - 5\n".getBytes()),
						ByteBuf.wrapForReading("Concurrent data - 6\n".getBytes()),
						ByteBuf.wrapForReading("Concurrent data - 7\n".getBytes()),
						ByteBuf.wrapForReading("Concurrent data - 8\n".getBytes()),
						ByteBuf.wrapForReading("Concurrent data - 9\n".getBytes()),
						ByteBuf.wrapForReading("Concurrent data #2\n".getBytes()),
						ByteBuf.wrapForReading("Concurrent data #2\n".getBytes()),
						ByteBuf.wrapForReading("Concurrent data #2\n".getBytes()),
						ByteBuf.wrapForReading("Concurrent data #2\n".getBytes())))
						.streamTo(ChannelConsumer.ofPromise(client.upload(file, 24))),

				delayed(Arrays.asList(
						ByteBuf.wrapForReading(" data - 1\n".getBytes()),
						ByteBuf.wrapForReading("Concurrent data - 2\n".getBytes()),
						ByteBuf.wrapForReading("Concurrent data - 3\n".getBytes()),
						ByteBuf.wrapForReading("Concurrent data - 4\n".getBytes()),
						ByteBuf.wrapForReading("Concurrent data - 5\n".getBytes()),
						ByteBuf.wrapForReading("Concurrent data - 6\n".getBytes()),
						ByteBuf.wrapForReading("Concurrent data - 7\n".getBytes()),
						ByteBuf.wrapForReading("Concurrent data - 8\n".getBytes()),
						ByteBuf.wrapForReading("Concurrent data - 9\n".getBytes()),
						ByteBuf.wrapForReading("Concurrent data #2\n".getBytes()),
						ByteBuf.wrapForReading("Concurrent data #2\n".getBytes()),
						ByteBuf.wrapForReading("Concurrent data #2\n".getBytes()),
						ByteBuf.wrapForReading("Concurrent data #2\n".getBytes()),
						ByteBuf.wrapForReading("Concurrent data + new line\n".getBytes())))
						.streamTo(ChannelConsumer.ofPromise(client.upload(file, 10)))
		);

		String expected = "Concurrent data - 1\n" +
				"Concurrent data - 2\n" +
				"Concurrent data - 3\n" +
				"Concurrent data - 4\n" +
				"Concurrent data - 5\n" +
				"Concurrent data - 6\n" +
				"Concurrent data - 7\n" +
				"Concurrent data - 8\n" +
				"Concurrent data - 9\n" +
				"Concurrent data #2\n" +
				"Concurrent data #2\n" +
				"Concurrent data #2\n" +
				"Concurrent data #2\n" +
				"Concurrent data + new line\n";

		String actual = await(ChannelSupplier.ofPromise(client.download(file))
				.toCollector(ByteBufQueue.collector())
				.map(buf -> buf.asString(UTF_8)));

		assertEquals(expected, actual);
	}

	private ChannelSupplier<ByteBuf> delayed(List<ByteBuf> list) {
		Iterator<ByteBuf> iterator = list.iterator();
		return ChannelSupplier.of(() ->
				iterator.hasNext() ?
						Promise.ofCallback(cb ->
								Eventloop.getCurrentEventloop()
										.delay(ThreadLocalRandom.current().nextInt(20) + 10, () ->
												cb.set(iterator.next()))) :
						Promise.of(null));
	}

	@Test
	public void testDoDownload() throws IOException {
		Path outputFile = clientPath.resolve("d.txt");

		ChannelSupplier<ByteBuf> supplier = await(client.download("2/b/d.txt"));
		await(supplier.streamTo(ChannelFileWriter.open(newCachedThreadPool(), outputFile)));

		assertArrayEquals(Files.readAllBytes(storagePath.resolve("2/b/d.txt")), Files.readAllBytes(outputFile));
	}

	@Test
	public void testDownloadNonExistingFile() {
		Throwable e = awaitException(client.download("no_file.txt"));

		assertSame(FILE_NOT_FOUND, e);
	}

	@Test
	public void testDeleteFile() {
		await(client.delete("2/3/a.txt"));

		assertFalse(Files.exists(storagePath.resolve("2/3/a.txt")));
	}

	@Test
	public void testDeleteNonExistingFile() {
		await(client.delete("no_file.txt"));
	}

	@Test
	public void testListFiles() {
		Set<String> expected = set(
				"1/a.txt",
				"1/b.txt",
				"2/3/a.txt",
				"2/b/d.txt",
				"2/b/e.txt"
		);

		List<FileMetadata> actual = await(client.list("**"));

		assertEquals(expected, actual.stream().map(FileMetadata::getName).collect(toSet()));
	}

	@Test
	public void testGlobListFiles() {
		Set<String> expected = set(
				"2/3/a.txt",
				"2/b/d.txt",
				"2/b/e.txt"
		);

		List<FileMetadata> actual = await(client.list("2/*/*.txt"));

		assertEquals(expected, actual.stream().map(FileMetadata::getName).collect(toSet()));
	}

	@Test
	public void testMove() throws IOException {
		byte[] expected = Files.readAllBytes(storagePath.resolve("1/a.txt"));
		await(client.move("1/a.txt", "3/new_folder/z.txt"));

		assertArrayEquals(expected, Files.readAllBytes(storagePath.resolve("3/new_folder/z.txt")));
		assertFalse(Files.exists(storagePath.resolve("1/a.txt")));
	}

	@Test
	public void testMoveIntoExisting() throws IOException {
		byte[] expected = Files.readAllBytes(storagePath.resolve("1/b.txt"));
		assertSame(FILE_EXISTS, awaitException(client.move("1/b.txt", "1/a.txt")));

		assertArrayEquals(expected, Files.readAllBytes(storagePath.resolve("1/b.txt")));
	}

	@Test
	public void testMoveNothingIntoNothing() {
		await(client.move("i_do_not_exist.txt", "neither_am_i.txt"));

		assertFalse(Files.exists(storagePath.resolve("i_do_not_exist.txt")));
		assertFalse(Files.exists(storagePath.resolve("neither_am_i.txt")));
	}

	@Test
	public void testMoveDirAtomicSpecialization() {
		await(client.moveDir("2", "3"));

		assertTrue(Files.isDirectory(storagePath.resolve("3")));
		assertFalse(Files.exists(storagePath.resolve("2")));
	}
}
