package io.datakernel.logfs;

import io.datakernel.async.SettableStage;
import io.datakernel.async.Stages;
import io.datakernel.bytebuf.ByteBuf;
import io.datakernel.eventloop.Eventloop;
import io.datakernel.serializer.BufferSerializer;
import io.datakernel.serializer.asm.BufferSerializers;
import io.datakernel.stream.*;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ExecutionException;

import static io.datakernel.eventloop.FatalErrorHandlers.rethrowOnAnyError;
import static io.datakernel.stream.DataStreams.stream;
import static io.datakernel.stream.StreamConsumers.ofStageWithResult;
import static io.datakernel.stream.StreamProducers.ofIterable;
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
		serializer = BufferSerializers.utf16Serializer();
	}

	@Test
	public void testConsumer() throws ExecutionException, InterruptedException {
		LogManager<String> logManager = LogManagerImpl.create(eventloop, logFileSystem, serializer);
		String testPartition = "testPartition";

		List<String> values = asList("test1", "test2", "test3");

		stream(ofIterable(values), ofStageWithResult(logManager.consumer(testPartition)));

		eventloop.run();

		StreamConsumerWithResult<String, List<String>> listConsumer = StreamConsumers.toList();
		stream(logManager.producerStream(testPartition, new LogFile("", 0), 0, null), listConsumer);

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
		public CompletionStage<LogFile> makeUniqueLogFile(String logPartition, String logName) {
			Map<LogFile, List<ByteBuf>> partition = partitions.computeIfAbsent(logPartition, s -> new HashMap<>());
			SettableStage<LogFile> stage = SettableStage.create();

			eventloop.schedule(eventloop.currentTimeMillis() + 100, () -> {
				LogFile value = new LogFile(logName, partition.size());
				partition.put(value, new ArrayList<>());
				stage.set(value);
			});

			return stage;
		}

		@Override
		public CompletionStage<List<LogFile>> list(String logPartition) {
			return Stages.of(new ArrayList<>(partitions.get(logPartition).keySet()));
		}

		@Override
		public CompletionStage<StreamProducerWithResult<ByteBuf, Void>> read(String logPartition, LogFile logFile, long startPosition) {
			List<ByteBuf> byteBufs = getOffset(partitions.get(logPartition).get(logFile), startPosition);
			StreamProducer<ByteBuf> producer = ofIterable(byteBufs);
			return Stages.of(StreamProducers.withEndOfStreamAsResult(producer));
		}

		private static List<ByteBuf> getOffset(List<ByteBuf> byteBufs, long startPosition) {
			if (startPosition == 0) return byteBufs;

			ArrayList<ByteBuf> offset = new ArrayList<>();
			for (ByteBuf byteBuf : byteBufs) {
				if (startPosition == 0) {
					offset.add(byteBuf);
				} else if (startPosition >= byteBuf.readRemaining()) {
					startPosition -= byteBuf.readRemaining();
				} else {
					byteBuf.moveReadPosition(((int) startPosition));
					startPosition = 0;
					offset.add(byteBuf);
				}
			}

			return offset;
		}

		@Override
		public CompletionStage<StreamConsumerWithResult<ByteBuf, Void>> write(String logPartition, LogFile logFile) {
			StreamConsumerWithResult<ByteBuf, List<ByteBuf>> listConsumer = StreamConsumers.toList();
			listConsumer.getResult().thenAccept(byteBufs -> partitions.get(logPartition).get(logFile).addAll(byteBufs));
			return Stages.of(StreamConsumers.withEndOfStreamAsResult(listConsumer));
		}
	}

}