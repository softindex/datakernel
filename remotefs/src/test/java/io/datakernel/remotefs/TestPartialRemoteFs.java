package io.datakernel.remotefs;

import io.datakernel.bytebuf.ByteBuf;
import io.datakernel.eventloop.Eventloop;
import io.datakernel.stream.StreamConsumerWithResult;
import io.datakernel.stream.StreamProducer;
import io.datakernel.stream.file.StreamFileWriter;
import io.datakernel.stream.processor.ByteBufRule;
import io.datakernel.test.TestUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Random;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static io.datakernel.eventloop.FatalErrorHandlers.rethrowOnAnyError;
import static io.datakernel.test.TestUtils.*;
import static java.nio.charset.StandardCharsets.UTF_8;
import static org.junit.Assert.assertArrayEquals;

public class TestPartialRemoteFs {
	static {
		TestUtils.enableLogging("io.datakernel.remotefs");
	}

	private static final int PORT = 5436;
	private static final String FILE = "file.txt";
	private static final byte[] CONTENT = "test content of the file".getBytes(UTF_8);

	private static final InetSocketAddress address = new InetSocketAddress("localhost", PORT);

	@Rule
	public ByteBufRule byteBufRule = new ByteBufRule();

	@Rule
	public TemporaryFolder tempFolder = new TemporaryFolder();

	private RemoteFsServer server;
	private Eventloop eventloop;
	private RemoteFsClient client;
	private ExecutorService executor;

	private Path serverStorage;
	private Path clientStorage;

	@Before
	public void setup() throws IOException {
		executor = Executors.newCachedThreadPool();
		eventloop = Eventloop.create()
				.withCurrentThread()
				.withFatalErrorHandler(rethrowOnAnyError());

		serverStorage = tempFolder.newFolder().toPath();
		clientStorage = tempFolder.newFolder().toPath();
		server = RemoteFsServer.create(eventloop, executor, serverStorage).withListenAddress(address);
		server.listen();
		client = RemoteFsClient.create(eventloop, address);

		Files.write(serverStorage.resolve(FILE), CONTENT);
	}

	@After
	public void tearDown() {
		server.close();
		executor.shutdownNow();
	}

	@Test
	public void justDownload() throws IOException {

		client.downloadStream(FILE).streamTo(StreamFileWriter.create(executor, clientStorage.resolve(FILE)))
				.getProducerResult()
				.whenComplete(($, err) -> server.close())
				.whenComplete(assertComplete());

		eventloop.run();

		assertArrayEquals(CONTENT, Files.readAllBytes(clientStorage.resolve(FILE)));
	}

	@Test
	public void ensuredUpload() throws IOException {
		enableLogging("io.datakernel.remotefs");

		byte[] data = new byte[10 * (1 << 20)]; // 10 mb
		new Random().nextBytes(data);

		StreamProducer<ByteBuf> producer = StreamProducer.of(ByteBuf.wrapForReading(data));
		StreamConsumerWithResult<ByteBuf, Void> consumer = client.uploadStream("test_big_file.bin", ".upload");

		producer.streamTo(consumer)
				.getConsumerResult()
				.whenComplete(($, err) -> server.close())
				.whenComplete(assertComplete());

		eventloop.run();

		assertArrayEquals(data, Files.readAllBytes(serverStorage.resolve("test_big_file.bin")));
	}

	@Test
	public void downloadPrefix() throws IOException {

		client.downloadStream(FILE, 0, 12).streamTo(StreamFileWriter.create(executor, clientStorage.resolve(FILE)))
				.getProducerResult()
				.whenComplete(($, err) -> server.close())
				.whenComplete(assertComplete());

		eventloop.run();

		assertArrayEquals("test content".getBytes(UTF_8), Files.readAllBytes(clientStorage.resolve(FILE)));
	}

	@Test
	public void downloadSuffix() throws IOException {

		client.downloadStream(FILE, 13).streamTo(StreamFileWriter.create(executor, clientStorage.resolve(FILE)))
				.getProducerResult()
				.whenComplete(($, err) -> server.close())
				.whenComplete(assertComplete());

		eventloop.run();

		assertArrayEquals("of the file".getBytes(UTF_8), Files.readAllBytes(clientStorage.resolve(FILE)));
	}

	@Test
	public void downloadPart() throws IOException {

		client.downloadStream(FILE, 5, 10).streamTo(StreamFileWriter.create(executor, clientStorage.resolve(FILE)))
				.getProducerResult()
				.whenComplete(($, err) -> server.close())
				.whenComplete(assertComplete());

		eventloop.run();

		assertArrayEquals("content of".getBytes(UTF_8), Files.readAllBytes(clientStorage.resolve(FILE)));
	}

	@Test
	public void downloadOverSuffix() throws IOException {

		client.downloadStream(FILE, 13, 123).streamTo(StreamFileWriter.create(executor, clientStorage.resolve(FILE)))
				.getProducerResult()
				.whenComplete(($, err) -> server.close())
				.whenComplete(assertFailure(RemoteFsException.class, "Boundaries exceed file size"));

		eventloop.run();
	}

	@Test
	public void downloadOver() throws IOException {

		client.downloadStream(FILE, 123, 123).streamTo(StreamFileWriter.create(executor, clientStorage.resolve(FILE)))
				.getProducerResult()
				.whenComplete(($, err) -> server.close())
				.whenComplete(assertFailure(RemoteFsException.class, "Offset exceeds file size"));

		eventloop.run();
	}
}
