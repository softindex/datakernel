package io.datakernel.remotefs;

import io.datakernel.csp.ChannelSupplier;
import io.datakernel.eventloop.Eventloop;
import io.datakernel.stream.processor.DatakernelRunner;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

import static io.datakernel.async.TestUtils.await;
import static io.datakernel.async.TestUtils.awaitException;
import static io.datakernel.bytebuf.ByteBufStrings.wrapUtf8;
import static io.datakernel.remotefs.FsClient.OFFSET_TOO_BIG;
import static java.nio.charset.StandardCharsets.UTF_8;
import static org.junit.Assert.*;

@RunWith(DatakernelRunner.class)
public final class TestLocalFsClientRevisions {

	@Rule
	public final TemporaryFolder tmpFolder = new TemporaryFolder();

	private Path storage;

	private FsClient client;

	@Before
	public void setUp() throws IOException {
		storage = tmpFolder.newFolder("storage").toPath();
		client = LocalFsClient.create(Eventloop.getCurrentEventloop(), storage).withRevisions();
	}

	@Test
	public void uploadOverride() throws IOException {
		await(ChannelSupplier.of(wrapUtf8("hello, this is first text")).streamTo(client.upload("test.txt", 0, 1)));
		await(ChannelSupplier.of(wrapUtf8("OVERRIDEN")).streamTo(client.upload("test.txt", 0, 2)));

		assertArrayEquals("OVERRIDEN".getBytes(UTF_8), Files.readAllBytes(storage.resolve("test.txt")));
	}

	@Test
	public void uploadAppend() throws IOException {
		await(ChannelSupplier.of(wrapUtf8("hello, this is first text")).streamTo(client.upload("test.txt", 0, 123)));
		await(ChannelSupplier.of(wrapUtf8("rst text and some appended text too")).streamTo(client.upload("test.txt", 17, 123)));

		assertArrayEquals("hello, this is first text and some appended text too".getBytes(UTF_8), Files.readAllBytes(storage.resolve("test.txt")));
	}

	@Test
	public void uploadOverrideAppend() {
		await(ChannelSupplier.of(wrapUtf8("hello, this is first text")).streamTo(client.upload("test.txt", 0, 1)));

		assertSame(OFFSET_TOO_BIG, awaitException(ChannelSupplier.of(wrapUtf8("rst text and some appended text too")).streamTo(client.upload("test.txt", 17, 2))));
	}

	@Test
	public void uploadDeleteUpload() throws IOException {
		await(ChannelSupplier.of(wrapUtf8("hello, this is first text")).streamTo(client.upload("test.txt", 0, 1)));
		await(client.delete("test.txt", 1));

		assertTrue(await(client.getMetadata("test.txt")).isTombstone());

		await(ChannelSupplier.of(wrapUtf8("OVERRIDEN")).streamTo(client.upload("test.txt", 0, 2)));

		assertFalse(await(client.getMetadata("test.txt")).isTombstone());

		assertArrayEquals("OVERRIDEN".getBytes(UTF_8), Files.readAllBytes(storage.resolve("test.txt")));
	}

	@Test
	public void lowRevisionDelete() {
		await(ChannelSupplier.of(wrapUtf8("hello, this is first text")).streamTo(client.upload("test.txt", 0, 10)));

		await(client.delete("test.txt", 1));

		assertFalse(await(client.getMetadata("test.txt")).isTombstone());
	}

	@Test
	public void deleteBeforeUpload() {
		await(client.delete("test.txt", 10));

		await(ChannelSupplier.of(wrapUtf8("hello, this is first text")).streamTo(client.upload("test.txt", 0, 1)));

		assertTrue(await(client.getMetadata("test.txt")).isTombstone());
	}

	@Test
	public void moveIntoLesser() throws IOException {
		await(ChannelSupplier.of(wrapUtf8("hello, this is some text")).streamTo(client.upload("test.txt", 0, 1)));
		await(ChannelSupplier.of(wrapUtf8("and this is another")).streamTo(client.upload("test2.txt", 0, 1)));

		await(client.move("test.txt", "test2.txt", 2, 2));

		assertTrue(await(client.getMetadata("test.txt")).isTombstone());

		System.out.println(await(client.listEntities("**")));
		assertArrayEquals("hello, this is some text".getBytes(UTF_8), Files.readAllBytes(storage.resolve("test2.txt")));
	}

	@Test
	public void moveIntoHigher() throws IOException {
		await(ChannelSupplier.of(wrapUtf8("hello, this is some text")).streamTo(client.upload("test.txt", 0, 1)));
		await(ChannelSupplier.of(wrapUtf8("and this is another")).streamTo(client.upload("test2.txt", 0, 10)));

		await(client.move("test.txt", "test2.txt", 2, 2));
		assertArrayEquals("and this is another".getBytes(UTF_8), Files.readAllBytes(storage.resolve("test2.txt")));
	}

	@Test
	public void copyIntoLesser() throws IOException {
		await(ChannelSupplier.of(wrapUtf8("hello, this is some text")).streamTo(client.upload("test.txt", 0, 1)));
		await(ChannelSupplier.of(wrapUtf8("and this is another")).streamTo(client.upload("test2.txt", 0, 1)));

		await(client.copy("test.txt", "test2.txt", 2));

		assertArrayEquals("hello, this is some text".getBytes(UTF_8), Files.readAllBytes(storage.resolve("test.txt")));
		assertArrayEquals("hello, this is some text".getBytes(UTF_8), Files.readAllBytes(storage.resolve("test2.txt")));
	}

	@Test
	public void copyIntoHigher() throws IOException {
		await(ChannelSupplier.of(wrapUtf8("hello, this is some text")).streamTo(client.upload("test.txt", 0, 1)));
		await(ChannelSupplier.of(wrapUtf8("and this is another")).streamTo(client.upload("test2.txt", 0, 10)));

		await(client.copy("test.txt", "test2.txt", 2));
		assertArrayEquals("and this is another".getBytes(UTF_8), Files.readAllBytes(storage.resolve("test2.txt")));
	}
}
