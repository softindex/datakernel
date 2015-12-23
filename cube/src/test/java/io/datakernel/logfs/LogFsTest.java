package io.datakernel.logfs;

import io.datakernel.async.*;
import io.datakernel.bytebuf.ByteBufPool;
import io.datakernel.eventloop.NioEventloop;
import io.datakernel.serializer.asm.BufferSerializers;
import io.datakernel.simplefs.SimpleFsClient;
import io.datakernel.simplefs.SimpleFsServer;
import io.datakernel.stream.StreamConsumers;
import io.datakernel.stream.StreamProducers;
import io.datakernel.time.SettableCurrentTimeProvider;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.net.InetSocketAddress;
import java.nio.file.Path;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static io.datakernel.async.AsyncCallbacks.ignoreCompletionCallback;
import static io.datakernel.bytebuf.ByteBufPool.getPoolItemsString;
import static io.datakernel.logfs.LogManagerImpl.DATE_TIME_FORMATTER;
import static java.util.Arrays.asList;
import static org.junit.Assert.assertEquals;

public class LogFsTest {
	private static final long ONE_MINUTE_MILLIS = 60 * 1000;
	private static final long ONE_HOUR_MILLIS = 60 * ONE_MINUTE_MILLIS;

	@Rule
	public TemporaryFolder temporaryFolder = new TemporaryFolder();
	private Path path;
	private SettableCurrentTimeProvider timeProvider;
	private ExecutorService executor;
	private NioEventloop eventloop;

	@Before
	public void setUp() throws Exception {
		path = temporaryFolder.newFolder().toPath();
		timeProvider = new SettableCurrentTimeProvider();
		executor = Executors.newCachedThreadPool();
		eventloop = new NioEventloop(timeProvider);
	}

	@Test
	public void testLocalFs() throws Exception {
		String logPartition = "p1";
		LocalFsLogFileSystem fileSystem = new LocalFsLogFileSystem(eventloop, executor, path);
		LogManager<String> logManager = new LogManagerImpl<>(eventloop, fileSystem, BufferSerializers.stringSerializer());

		timeProvider.setTime(0); // 00:00
		new StreamProducers.OfIterator<>(eventloop, asList("1", "2", "3").iterator())
				.streamTo(logManager.consumer(logPartition));
		eventloop.run();

		timeProvider.setTime(ONE_HOUR_MILLIS - 15 * ONE_MINUTE_MILLIS); // 00:45
		new StreamProducers.OfIterator<>(eventloop, asList("4", "5", "6").iterator())
				.streamTo(logManager.consumer(logPartition));
		eventloop.run();

		timeProvider.setTime(2 * ONE_HOUR_MILLIS - 15 * ONE_MINUTE_MILLIS); // 01:45
		new StreamProducers.OfIterator<>(eventloop, asList("7", "8", "9").iterator())
				.streamTo(logManager.consumer(logPartition));
		eventloop.run();

		timeProvider.setTime(3 * ONE_HOUR_MILLIS - 30 * ONE_MINUTE_MILLIS); // 02:30
		new StreamProducers.OfIterator<>(eventloop, asList("10", "11", "12").iterator())
				.streamTo(logManager.consumer(logPartition));
		eventloop.run();

		timeProvider.setTime(4 * ONE_HOUR_MILLIS - 45 * ONE_MINUTE_MILLIS); // 03:15
		new StreamProducers.OfIterator<>(eventloop, asList("13", "14", "15").iterator())
				.streamTo(logManager.consumer(logPartition));
		eventloop.run();

		LogStreamProducer<String> producer1 = logManager.producer(logPartition, ONE_HOUR_MILLIS, 2 * ONE_HOUR_MILLIS - 1); // from 01:00 to 01:59:59
		StreamConsumers.ToList<String> consumer1 = new StreamConsumers.ToList<>(eventloop);
		producer1.streamTo(consumer1);
		eventloop.run();
		assertEquals(asList("7", "8", "9"), consumer1.getList());

		ResultCallbackFuture<LogPosition> positionFuture = new ResultCallbackFuture<>();
		LogStreamProducer<String> producer2 = logManager.producer(logPartition,
				new LogFile(DATE_TIME_FORMATTER.print(0), 1), 0,
				new LogFile(DATE_TIME_FORMATTER.print(2 * ONE_HOUR_MILLIS), 0), positionFuture);
		StreamConsumers.ToList<String> consumer2 = new StreamConsumers.ToList<>(eventloop);
		producer2.streamTo(consumer2);
		eventloop.run();
		assertEquals(path.resolve(DATE_TIME_FORMATTER.print(2 * ONE_HOUR_MILLIS) + "." + logPartition + ".log").toFile().length(), positionFuture.get().getPosition());
		assertEquals(new LogFile(DATE_TIME_FORMATTER.print(2 * ONE_HOUR_MILLIS), 0), positionFuture.get().getLogFile());
		assertEquals(asList("4", "5", "6", "7", "8", "9", "10", "11", "12"), consumer2.getList());

		assertEquals(getPoolItemsString(), ByteBufPool.getCreatedItems(), ByteBufPool.getPoolItems());
	}

	@Test
	public void testSimpleFs() throws Exception {
		String logName = "log";
		InetSocketAddress address = new InetSocketAddress(33333);
		final SimpleFsServer server = createServer(address, path);
		SimpleFsClient client = createClient(address);

		LogFileSystem fileSystem = new SimpleFsLogFileSystem(client, logName);
		final LogManager<String> logManager = new LogManagerImpl<>(eventloop, fileSystem, BufferSerializers.stringSerializer());

		CompletionCallback stopCallback = new SimpleCompletionCallback() {
			@Override
			protected void onCompleteOrException() {
				server.stop(ignoreCompletionCallback());
			}
		};

		timeProvider.setTime(0); // 00:00
		startServer(server);
		new StreamProducers.OfIterator<>(eventloop, asList("1", "3", "5").iterator())
				.streamTo(logManager.consumer("p1", stopCallback));
		eventloop.run();
		startServer(server);
		new StreamProducers.OfIterator<>(eventloop, asList("2", "4", "6").iterator())
				.streamTo(logManager.consumer("p2", stopCallback));
		eventloop.run();

		timeProvider.setTime(2 * ONE_HOUR_MILLIS - 15 * ONE_MINUTE_MILLIS); // 01:45
		startServer(server);
		new StreamProducers.OfIterator<>(eventloop, asList("7", "9", "11").iterator())
				.streamTo(logManager.consumer("p1", stopCallback));
		eventloop.run();
		startServer(server);
		new StreamProducers.OfIterator<>(eventloop, asList("8", "10", "12").iterator())
				.streamTo(logManager.consumer("p2", stopCallback));
		eventloop.run();

		timeProvider.setTime(2 * ONE_HOUR_MILLIS + 15 * ONE_MINUTE_MILLIS); // 02:15
		startServer(server);
		new StreamProducers.OfIterator<>(eventloop, asList("13", "15", "17").iterator())
				.streamTo(logManager.consumer("p1", stopCallback));
		eventloop.run();
		startServer(server);
		new StreamProducers.OfIterator<>(eventloop, asList("14", "16", "18").iterator())
				.streamTo(logManager.consumer("p2", stopCallback));
		eventloop.run();

		startServer(server);
		LogStreamProducer<String> producer = logManager.producer("p1",
				new LogFile(DATE_TIME_FORMATTER.print(ONE_HOUR_MILLIS), 0), 0,
				AsyncCallbacks.<LogPosition>ignoreResultCallback()); // from 01:00
		StreamConsumers.ToList<String> consumer = new StreamConsumers.ToList<>(eventloop);
		producer.streamTo(consumer);
		consumer.setCompletionCallback(stopCallback);
		eventloop.run();

		assertEquals(asList("7", "9", "11", "13", "15", "17"), consumer.getList());
	}

	private void startServer(SimpleFsServer server) throws ExecutionException, InterruptedException {
		CompletionCallbackFuture startFuture = new CompletionCallbackFuture();
		server.start(startFuture);
		startFuture.get();
	}

	private SimpleFsServer createServer(InetSocketAddress address, Path serverStorage) {
		return SimpleFsServer.buildInstance(eventloop, executor, serverStorage)
				.specifyListenAddress(address)
				.build();
	}

	private SimpleFsClient createClient(InetSocketAddress address) {
		return SimpleFsClient.buildInstance(eventloop, address).build();
	}
}
