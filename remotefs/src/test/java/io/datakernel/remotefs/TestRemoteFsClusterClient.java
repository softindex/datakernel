package io.datakernel.remotefs;

import io.datakernel.async.AsyncConsumer;
import io.datakernel.async.Stages;
import io.datakernel.bytebuf.ByteBuf;
import io.datakernel.eventloop.AbstractServer;
import io.datakernel.eventloop.Eventloop;
import io.datakernel.serial.SerialConsumer;
import io.datakernel.serial.SerialSupplier;
import io.datakernel.serial.file.SerialFileWriter;
import io.datakernel.stream.processor.ByteBufRule;
import org.junit.*;
import org.junit.rules.TemporaryFolder;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.stream.IntStream;

import static io.datakernel.eventloop.FatalErrorHandlers.rethrowOnAnyError;
import static io.datakernel.test.TestUtils.assertComplete;
import static io.datakernel.test.TestUtils.assertFailure;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.nio.file.Files.readAllBytes;
import static java.util.Collections.singletonList;
import static java.util.concurrent.Executors.newCachedThreadPool;
import static org.junit.Assert.assertEquals;

public class TestRemoteFsClusterClient {
	public static final int CLIENT_SERVER_PAIRS = 10;

	@Rule
	public final TemporaryFolder tmpFolder = new TemporaryFolder();

	@Rule
	public final ByteBufRule byteBufRule = new ByteBufRule();

	private final Path[] serverStorages = new Path[CLIENT_SERVER_PAIRS];

	private ExecutorService executor;
	private Eventloop eventloop;
	private List<RemoteFsServer> servers;
	private Path clientStorage;
	private RemoteFsClusterClient client;

	@Before
	public void setup() throws IOException {
		executor = newCachedThreadPool();
		eventloop = Eventloop.create().withCurrentThread().withFatalErrorHandler(rethrowOnAnyError());
		servers = new ArrayList<>(CLIENT_SERVER_PAIRS);
		clientStorage = Paths.get(tmpFolder.newFolder("client").toURI());

		Files.createDirectories(clientStorage);

		Map<Object, FsClient> clients = new HashMap<>(CLIENT_SERVER_PAIRS);

		for (int i = 0; i < CLIENT_SERVER_PAIRS; i++) {
			InetSocketAddress address = new InetSocketAddress("localhost", 5600 + i);

			clients.put("server_" + i, RemoteFsClient.create(eventloop, address));

			serverStorages[i] = Paths.get(tmpFolder.newFolder("storage_" + i).toURI());
			Files.createDirectories(serverStorages[i]);

			RemoteFsServer server = RemoteFsServer.create(eventloop, executor, serverStorages[i]).withListenAddress(address);
			server.listen();
			servers.add(server);
		}

		clients.put("dead_one", RemoteFsClient.create(eventloop, new InetSocketAddress("localhost", 5555)));
		clients.put("dead_two", RemoteFsClient.create(eventloop, new InetSocketAddress("localhost", 5556)));
		clients.put("dead_three", RemoteFsClient.create(eventloop, new InetSocketAddress("localhost", 5557)));
		client = RemoteFsClusterClient.create(eventloop, clients);
		client.withReplicationCount(4); // there are those 3 dead nodes added above

		eventloop.delayBackground(10_000, () -> Assert.fail("Timeout"));
	}

	@Test
	public void testUpload() throws IOException {
		String content = "test content of the file";
		String resultFile = "file.txt";

		SerialSupplier.of(ByteBuf.wrapForReading(content.getBytes(UTF_8))).streamTo(client.uploadSerial(resultFile))
				.whenComplete(($, e) -> servers.forEach(AbstractServer::close))
				.whenComplete(assertComplete());

		eventloop.run();

		int uploaded = 0;
		for (int i = 0; i < CLIENT_SERVER_PAIRS; i++) {
			Path path = serverStorages[i].resolve(resultFile);
			if (Files.exists(path)) {
				assertEquals(new String(readAllBytes(path), UTF_8), content);
				uploaded++;
			}
		}
		assertEquals(4, uploaded); // replication count
	}

	@Test
	public void testDownload() throws IOException {
		int numOfServer = 3;
		String file = "the_file.txt";
		String content = "another test content of the file";

		Files.write(serverStorages[numOfServer].resolve(file), content.getBytes(UTF_8));

		SerialSupplier<ByteBuf> producer = client.downloadSerial(file, 0);
		SerialFileWriter consumer = SerialFileWriter.create(executor, clientStorage.resolve(file));

		producer.streamTo(consumer)
				.whenComplete(($, e) -> servers.forEach(AbstractServer::close))
				.whenComplete(assertComplete());

		eventloop.run();

		assertEquals(new String(readAllBytes(clientStorage.resolve(file)), UTF_8), content);
	}

	@Test
	public void testUploadSelector() throws IOException {
		String content = "test content of the file";
		ByteBuf data = ByteBuf.wrapForReading(content.getBytes(UTF_8));

		client.withReplicationCount(1)
				.withServerSelector((fileName, serverKeys, topShards) -> { // topShards are replication count, so they are 1 here
					if (fileName.contains("1")) {
						return singletonList("server_1");
					}
					if (fileName.contains("2")) {
						return singletonList("server_2");
					}
					if (fileName.contains("3")) {
						return singletonList("server_3");
					}
					return singletonList("server_0");
				});

		String[] files = {"file_1.txt", "file_2.txt", "file_3.txt", "other.txt"};

		Stages.all(Arrays.stream(files).map(f -> SerialSupplier.of(data.slice()).streamTo(client.uploadSerial(f))))
				.whenComplete(($, e) -> servers.forEach(AbstractServer::close))
				.whenComplete(assertComplete());

		eventloop.run();
		data.recycle();

		assertEquals(new String(readAllBytes(serverStorages[1].resolve("file_1.txt")), UTF_8), content);
		assertEquals(new String(readAllBytes(serverStorages[2].resolve("file_2.txt")), UTF_8), content);
		assertEquals(new String(readAllBytes(serverStorages[3].resolve("file_3.txt")), UTF_8), content);
		assertEquals(new String(readAllBytes(serverStorages[0].resolve("other.txt")), UTF_8), content);
	}

	@Test
	@Ignore
	// this test uses lots of local ports (and all of them are in TIME_WAIT state after it for a minute) so HTTP tests after it may fail indefinitely
	public void testUploadAlot() throws IOException {
		String content = "test content of the file";
		ByteBuf data = ByteBuf.wrapForReading(content.getBytes(UTF_8));

		Stages.runSequence(IntStream.range(0, 1000)
				.mapToObj(i -> SerialSupplier.of(data.slice()).streamTo(client.uploadSerial("file_uploaded_" + i + ".txt"))))
				.whenComplete(($, e) -> servers.forEach(AbstractServer::close))
				.whenComplete(assertComplete());

		eventloop.run();
		data.recycle();

		for (int i = 0; i < CLIENT_SERVER_PAIRS; i++) {
			for (int j = 0; j < 1000; j++) {
				assertEquals(new String(readAllBytes(serverStorages[i].resolve("file_uploaded_" + i + ".txt")), UTF_8), content);
			}
		}
	}

	@Test
	public void testNotEnoughUploads() {
		client.withReplicationCount(client.getClients().size()); // max possible replication

		SerialSupplier.of(ByteBuf.wrapForReading("whatever, blah-blah".getBytes(UTF_8))).streamTo(client.uploadSerial("file_uploaded.txt"))
				.whenComplete(($, e) -> servers.forEach(AbstractServer::close))
				.whenComplete(assertFailure(RemoteFsException.class, "Didn't connect to enough partitions"));

		eventloop.run();
	}

	@Test
	public void downloadNonExisting() {

		String fileName = "i_dont_exist.txt";

		client.downloadSerial(fileName)
				.streamTo(SerialConsumer.of(AsyncConsumer.of(ByteBuf::recycle)))
				.whenComplete(($, e) -> servers.forEach(AbstractServer::close))
				.whenComplete(assertFailure(RemoteFsException.class, fileName));

		eventloop.run();
	}
}
