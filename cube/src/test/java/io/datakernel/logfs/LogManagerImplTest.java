package io.datakernel.logfs;

import io.datakernel.async.AsyncConsumer;
import io.datakernel.async.Promise;
import io.datakernel.async.SettablePromise;
import io.datakernel.bytebuf.ByteBuf;
import io.datakernel.bytebuf.ByteBufQueue;
import io.datakernel.csp.ChannelConsumer;
import io.datakernel.csp.ChannelSupplier;
import io.datakernel.eventloop.Eventloop;
import io.datakernel.serializer.BufferSerializer;
import io.datakernel.serializer.asm.BufferSerializers;
import io.datakernel.stream.StreamConsumerToList;
import io.datakernel.stream.StreamSupplier;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

import static io.datakernel.eventloop.FatalErrorHandlers.rethrowOnAnyError;
import static java.util.Arrays.asList;
import static org.junit.Assert.assertEquals;

public class LogManagerImplTest {
	private Eventloop eventloop;
	private LogFileSystem logFileSystem;
	private BufferSerializer<String> serializer;

	@Before
	public void before() {
		eventloop = Eventloop.create().withFatalErrorHandler(rethrowOnAnyError()).withCurrentThread();
		logFileSystem = new LogFileSystemStub(eventloop);
		serializer = BufferSerializers.UTF16_SERIALIZER;
	}

	@Test
	public void testConsumer() throws ExecutionException, InterruptedException {
		LogManager<String> logManager = LogManagerImpl.create(eventloop, logFileSystem, serializer);
		String testPartition = "testPartition";

		List<String> values = asList("test1", "test2", "test3");

		StreamSupplier.ofIterable(values)
				.streamTo(logManager.consumerStream(testPartition));

		eventloop.run();

		StreamConsumerToList<String> listConsumer = StreamConsumerToList.create();
		logManager.supplierStream(testPartition, new LogFile("", 0), 0, null)
				.getSupplier()
				.streamTo(listConsumer);

		CompletableFuture<List<String>> listFuture = listConsumer.getResult().toCompletableFuture();
		eventloop.run();
		assertEquals(values, listFuture.get());
	}

	private static class LogFileSystemStub implements LogFileSystem {
		private final Eventloop eventloop;

		private final Map<String, Map<LogFile, List<ByteBuf>>> partitions = new HashMap<>();

		private LogFileSystemStub(Eventloop eventloop) {
			this.eventloop = eventloop;
		}

		@Override
		public Promise<LogFile> makeUniqueLogFile(String logPartition, String logName) {
			Map<LogFile, List<ByteBuf>> partition = partitions.computeIfAbsent(logPartition, s -> new HashMap<>());
			SettablePromise<LogFile> promise = new SettablePromise<>();

			eventloop.delay(100, () -> {
				LogFile value = new LogFile(logName, partition.size());
				partition.put(value, new ArrayList<>());
				promise.set(value);
			});

			return promise;
		}

		@Override
		public Promise<List<LogFile>> list(String logPartition) {
			return Promise.of(new ArrayList<>(partitions.get(logPartition).keySet()));
		}

		@Override
		public Promise<ChannelSupplier<ByteBuf>> read(String logPartition, LogFile logFile, long startPosition) {
			List<ByteBuf> byteBufs = getOffset(partitions.get(logPartition).get(logFile), startPosition);
			return Promise.of(ChannelSupplier.ofIterable(byteBufs));
		}

		private static List<ByteBuf> getOffset(List<ByteBuf> byteBufs, long startPosition) {
			ByteBufQueue bufs = new ByteBufQueue();
			byteBufs.forEach(buf -> bufs.add(buf.slice()));
			bufs.skip((int) startPosition);
			List<ByteBuf> result = new ArrayList<>();
			while (!bufs.isEmpty()) {
				result.add(bufs.take());
			}
			return result;
		}

		@Override
		public Promise<ChannelConsumer<ByteBuf>> write(String logPartition, LogFile logFile) {
			List<ByteBuf> bufs = partitions.get(logPartition).get(logFile);
			return Promise.of(ChannelConsumer.of(AsyncConsumer.of(bufs::add)));
		}
	}

}
