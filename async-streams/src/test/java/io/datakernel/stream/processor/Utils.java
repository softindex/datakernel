package io.datakernel.stream.processor;

import io.datakernel.stream.*;

import java.util.Arrays;
import java.util.List;

import static org.junit.Assert.assertEquals;

public class Utils {
	private Utils() {
	}

	public static StreamStatus[] producerStatuses(List<? extends StreamProducer<?>> streamProducers) {
		StreamStatus[] result = new StreamStatus[streamProducers.size()];
		for (int i = 0; i < streamProducers.size(); i++) {
			StreamProducer<?> streamProducer = streamProducers.get(i);
			result[i] = streamProducer.getProducerStatus();
		}
		return result;
	}

	public static StreamStatus[] consumerStatuses(List<? extends StreamConsumer<?>> streamConsumers) {
		StreamStatus[] result = new StreamStatus[streamConsumers.size()];
		for (int i = 0; i < streamConsumers.size(); i++) {
			StreamConsumer<?> streamConsumer = streamConsumers.get(i);
			result[i] = streamConsumer.getConsumerStatus();
		}
		return result;
	}

	public static void assertConsumerStatuses(StreamStatus expected, List<? extends StreamConsumer<?>> streamConsumers) {
		StreamStatus[] statuses = consumerStatuses(streamConsumers);
		for (StreamStatus status : statuses) {
			assertEquals("Expected " + expected + ", actual: " + Arrays.toString(statuses), expected, status);
		}
	}

	public static void assertProducerStatuses(StreamStatus expected, List<? extends StreamProducer<?>> streamProducers) {
		StreamStatus[] statuses = producerStatuses(streamProducers);
		for (StreamStatus status : statuses) {
			assertEquals("Expected " + expected + ", actual: " + Arrays.toString(statuses), expected, status);
		}
	}

}
