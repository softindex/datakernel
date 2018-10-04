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

package io.datakernel.remotefs;

import io.datakernel.async.AsyncConsumer;
import io.datakernel.async.Stage;
import io.datakernel.async.Stages;
import io.datakernel.bytebuf.ByteBuf;
import io.datakernel.eventloop.Eventloop;
import io.datakernel.file.AsyncFile;
import io.datakernel.serial.SerialConsumer;
import io.datakernel.serial.SerialSupplier;
import io.datakernel.serial.file.SerialFileWriter;
import io.datakernel.stream.processor.ByteBufRule;
import io.datakernel.util.MemSize;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.rules.TemporaryFolder;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;
import java.util.concurrent.ExecutorService;

import static io.datakernel.bytebuf.ByteBufPool.*;
import static io.datakernel.eventloop.FatalErrorHandlers.rethrowOnAnyError;
import static io.datakernel.serial.file.SerialFileReader.READ_OPTIONS;
import static io.datakernel.serial.file.SerialFileReader.readFile;
import static io.datakernel.serial.file.SerialFileWriter.CREATE_OPTIONS;
import static io.datakernel.test.TestUtils.assertComplete;
import static io.datakernel.test.TestUtils.assertFailure;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.nio.file.StandardOpenOption.CREATE;
import static java.nio.file.StandardOpenOption.TRUNCATE_EXISTING;
import static java.util.Arrays.asList;
import static java.util.concurrent.Executors.newCachedThreadPool;
import static org.junit.Assert.*;

public class TestLocalFsClient {
	private static final MemSize bufferSize = MemSize.of(2);

	@Rule
	public ByteBufRule byteBufRule = new ByteBufRule();

	@Rule
	public final TemporaryFolder tmpFolder = new TemporaryFolder();

	@Rule
	public final ExpectedException thrown = ExpectedException.none();

	private Eventloop eventloop = Eventloop.create().withFatalErrorHandler(rethrowOnAnyError()).withCurrentThread();
	private ExecutorService executor = newCachedThreadPool();
	private Path storagePath;
	private Path clientPath;

	private LocalFsClient client;

	@Before
	public void setup() throws IOException {
//		Runtime.getRuntime().exec("rm -r /tmp/TEST/").waitFor();
//		storagePath = Paths.get("/tmp/TEST");
		storagePath = Paths.get(tmpFolder.newFolder("storage").toURI());
		clientPath = Paths.get(tmpFolder.newFolder("client").toURI());

		Files.createDirectories(storagePath);
		Files.createDirectories(clientPath);

		Path f = clientPath.resolve("f.txt");
		Files.write(f, ("some text1\n\nmore text1\t\n\n\r").getBytes(UTF_8), CREATE, TRUNCATE_EXISTING);

		Path c = clientPath.resolve("c.txt");
		Files.write(c, ("some text2\n\nmore text2\t\n\n\r").getBytes(UTF_8), CREATE, TRUNCATE_EXISTING);

		Files.createDirectories(storagePath.resolve("1"));
		Files.createDirectories(storagePath.resolve("2/3"));
		Files.createDirectories(storagePath.resolve("2/b"));

		Path a1 = storagePath.resolve("1/a.txt");
		Files.write(a1, ("1\n2\n3\n4\n5\n6\n").getBytes(UTF_8), CREATE, TRUNCATE_EXISTING);

		Path b = storagePath.resolve("1/b.txt");
		Files.write(b, ("7\n8\n9\n10\n11\n12\n").getBytes(UTF_8), CREATE, TRUNCATE_EXISTING);

		Path a2 = storagePath.resolve("2/3/a.txt");
		Files.write(a2, ("6\n5\n4\n3\n2\n1\n").getBytes(UTF_8), CREATE, TRUNCATE_EXISTING);

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

		client = LocalFsClient.create(eventloop, executor, storagePath);
	}

	@After
	public void tearDown() {
		executor.shutdownNow();
	}

	@Test
	public void testDoUpload() throws IOException {
		Path inputFile = clientPath.resolve("c.txt");

		AsyncFile file = AsyncFile.open(executor, inputFile, READ_OPTIONS);

		client.upload("1/c.txt").whenResult(consumer ->
				readFile(file).withBufferSize(bufferSize)
						.streamTo(consumer)
						.whenComplete(assertComplete()));

		eventloop.run();

		assertArrayEquals(Files.readAllBytes(inputFile), Files.readAllBytes(storagePath.resolve("1/c.txt")));
	}

	@Test
	public void testConcurrentUpload() throws IOException {
		Files.write(storagePath.resolve("concurrent.txt"), "Concurrent data - 1\nConcurr".getBytes());

		Stages.all(
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
						.streamTo(client.uploadSerial("concurrent.txt", 1)),

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
						.streamTo(client.uploadSerial("concurrent.txt", 10)),

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
						.streamTo(client.uploadSerial("concurrent.txt", 15)),

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
						.streamTo(client.uploadSerial("concurrent.txt", 24)),

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
						.streamTo(client.uploadSerial("concurrent.txt", 10))
		)
				.thenCompose($ ->
						client.downloadSerial("concurrent.txt")
								.streamTo(SerialConsumer.of(AsyncConsumer.of(buf -> {
									String actual = buf.asString(UTF_8);
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
									assertEquals(expected, actual);
								}))))
				.whenComplete(($, e) -> System.out.println("finished"))
				.whenComplete(assertComplete());

		eventloop.run();

	}

	private SerialSupplier<ByteBuf> delayed(List<ByteBuf> list) {
		Random random = new Random();
		Iterator<ByteBuf> iterator = list.iterator();
		return SerialSupplier.of(() ->
				iterator.hasNext() ?
						Stage.ofCallback(stage ->
								eventloop.delay(random.nextInt(20) + 10, () ->
										stage.set(iterator.next()))) :
						Stage.of(null));
	}

	@Test
	public void testDoDownload() throws IOException {
		Path outputFile = clientPath.resolve("d.txt");

		AsyncFile open = AsyncFile.open(executor, outputFile, CREATE_OPTIONS);
		client.download("2/b/d.txt")
				.whenResult(reader -> reader.streamTo(SerialFileWriter.create(open))
						.whenComplete(assertComplete()));

		eventloop.run();

		assertArrayEquals(Files.readAllBytes(storagePath.resolve("2/b/d.txt")), Files.readAllBytes(outputFile));
	}

	@Test
	public void testDownloadNonExistingFile() {
		String fileName = "no_file.txt";
		client.downloadSerial(fileName)
				.withEndOfStream(eos ->
						eos.whenComplete((result, error) -> {
							assertNotNull(error);
							assertEquals(error.getClass(), RemoteFsException.class);
							assertTrue(error.getMessage().contains(fileName));
						}));

		eventloop.run();
	}

	@Test
	public void testDeleteFile() {
		assertTrue(Files.exists(storagePath.resolve("2/3/a.txt")));

		client.delete("2/3/a.txt").whenComplete(assertComplete());
		eventloop.run();

		assertFalse(Files.exists(storagePath.resolve("2/3/a.txt")));
	}

	@Test
	public void testDeleteNonExistingFile() {
		client.delete("no_file.txt").whenComplete(assertComplete());
		eventloop.run();
	}

	@Test
	public void testListFiles() {
		List<FileMetadata> expected = asList(
				new FileMetadata("1/a.txt", 12, 0),
				new FileMetadata("1/b.txt", 15, 0),
				new FileMetadata("2/3/a.txt", 12, 0),
				new FileMetadata("2/b/d.txt", 6888890, 0),
				new FileMetadata("2/b/e.txt", 0, 0)
		);
		List<FileMetadata> actual = new ArrayList<>();

		client.list().whenComplete(assertComplete(actual::addAll));
		eventloop.run();

		Comparator<FileMetadata> comparator = Comparator.comparing(FileMetadata::getName);
		expected.sort(comparator);
		actual.sort(comparator);

		assertEquals(expected.size(), actual.size());
		for (int i = 0; i < expected.size(); i++) {
			assertTrue(expected.get(i).equalsIgnoringTimestamp(actual.get(i)));
		}

		assertEquals(getPoolItemsString(), getCreatedItems(), getPoolItems());
	}

	@Test
	public void testGlobListFiles() {
		List<FileMetadata> expected = asList(
				new FileMetadata("2/3/a.txt", 12, 0),
				new FileMetadata("2/b/d.txt", 6888890, 0),
				new FileMetadata("2/b/e.txt", 0, 0)
		);
		List<FileMetadata> actual = new ArrayList<>();

		client.list("2/*/*.txt").whenComplete(assertComplete(actual::addAll));
		eventloop.run();

		Comparator<FileMetadata> comparator = Comparator.comparing(FileMetadata::getName);
		expected.sort(comparator);
		actual.sort(comparator);

		assertEquals(expected.size(), actual.size());
		for (int i = 0; i < expected.size(); i++) {
			assertTrue(expected.get(i).equalsIgnoringTimestamp(actual.get(i)));
		}
	}

	@Test
	public void testMove() throws IOException {

		byte[] expected = Files.readAllBytes(storagePath.resolve("1/a.txt"));

		client.move("1/a.txt", "3/new_folder/z.txt").whenComplete(assertComplete());
		eventloop.run();

		assertArrayEquals(expected, Files.readAllBytes(storagePath.resolve("3/new_folder/z.txt")));
		assertFalse(Files.exists(storagePath.resolve("1/a.txt")));
	}

	@Test
	public void testPossiblyPreviousSuccessfulMove() throws IOException {

		byte[] expected = Files.readAllBytes(storagePath.resolve("1/a.txt"));

		client.move("3/new_folder/z.txt", "1/a.txt").whenComplete(assertComplete());
		eventloop.run();

		assertArrayEquals(expected, Files.readAllBytes(storagePath.resolve("1/a.txt")));
		assertFalse(Files.exists(storagePath.resolve("3/new_folder/z.txt")));
	}

	@Test
	public void testMoveBiggerIntoSmaller() throws IOException {
//		1/a.txt -> 12 bytes
//		1/b.txt -> 15 bytes

		byte[] expected = Files.readAllBytes(storagePath.resolve("1/b.txt"));

		client.move("1/b.txt", "1/a.txt").whenComplete(assertComplete());
		eventloop.run();

		assertArrayEquals(expected, Files.readAllBytes(storagePath.resolve("1/a.txt")));
		assertFalse(Files.exists(storagePath.resolve("1/b.txt")));
	}

	@Test
	public void testMoveSmallerIntoBigger() throws IOException {
		byte[] expected = Files.readAllBytes(storagePath.resolve("1/b.txt"));

		client.move("1/a.txt", "1/b.txt").whenComplete(assertComplete());
		eventloop.run();

		assertArrayEquals(expected, Files.readAllBytes(storagePath.resolve("1/b.txt")));
		assertFalse(Files.exists(storagePath.resolve("1/a.txt")));
	}

	@Test
	public void testMoveNothingIntoNothing() {

		client.move("i_do_not_exist.txt", "neither_am_i.txt")
				.whenComplete(assertFailure(RemoteFsException.class, e ->
						assertTrue(e.getMessage().matches("No file .*?, neither file .*? were found"))));

		eventloop.run();

		assertFalse(Files.exists(storagePath.resolve("i_do_not_exist.txt")));
		assertFalse(Files.exists(storagePath.resolve("neither_am_i.txt")));
	}
}
