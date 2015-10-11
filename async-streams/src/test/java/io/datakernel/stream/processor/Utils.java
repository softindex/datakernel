package io.datakernel.stream.processor;

import io.datakernel.stream.*;

import java.util.Arrays;
import java.util.List;

import static org.junit.Assert.assertEquals;

public class Utils {
	private Utils() {
	}

	public static AbstractStreamConsumer.StreamConsumerStatus consumerStatus(StreamConsumer streamConsumer) {
		if (streamConsumer instanceof AbstractStreamTransformer_1_1)
			streamConsumer = ((AbstractStreamTransformer_1_1) streamConsumer).getUpstreamConsumer();
		if (streamConsumer instanceof AbstractStreamTransformer_1_N)
			streamConsumer = ((AbstractStreamTransformer_1_N) streamConsumer).getUpstreamConsumer();
		if (streamConsumer instanceof AbstractStreamConsumer)
			return ((AbstractStreamConsumer) streamConsumer).getStatus();
		throw new IllegalArgumentException();
	}

	public static AbstractStreamProducer.StreamProducerStatus producerStatus(StreamProducer streamProducer) {
		if (streamProducer instanceof AbstractStreamTransformer_1_1)
			streamProducer = ((AbstractStreamTransformer_1_1) streamProducer).getDownstreamProducer();
		if (streamProducer instanceof AbstractStreamTransformer_M_1)
			streamProducer = ((AbstractStreamTransformer_M_1) streamProducer).getDownstreamProducer();
		if (streamProducer instanceof AbstractStreamProducer)
			return ((AbstractStreamProducer) streamProducer).getStatus();
		throw new IllegalArgumentException();
	}

	public static AbstractStreamProducer.StreamProducerStatus[] producerStatuses(List<? extends StreamProducer<?>> streamProducers) {
		AbstractStreamProducer.StreamProducerStatus[] result = new AbstractStreamProducer.StreamProducerStatus[streamProducers.size()];
		for (int i = 0; i < streamProducers.size(); i++) {
			StreamProducer<?> streamProducer = streamProducers.get(i);
			result[i] = producerStatus(streamProducer);
		}
		return result;
	}

	public static AbstractStreamConsumer.StreamConsumerStatus[] consumerStatuses(List<? extends StreamConsumer<?>> streamConsumers) {
		AbstractStreamConsumer.StreamConsumerStatus[] result = new AbstractStreamConsumer.StreamConsumerStatus[streamConsumers.size()];
		for (int i = 0; i < streamConsumers.size(); i++) {
			StreamConsumer<?> streamConsumer = streamConsumers.get(i);
			result[i] = consumerStatus(streamConsumer);
		}
		return result;
	}

	public static void assertStatus(AbstractStreamConsumer.StreamConsumerStatus expected, StreamConsumer<?> consumer) {
		AbstractStreamConsumer.StreamConsumerStatus actual = consumerStatus(consumer);
		assertEquals(expected, actual);
	}

	public static void assertStatus(AbstractStreamProducer.StreamProducerStatus expected, StreamProducer<?> producer) {
		AbstractStreamProducer.StreamProducerStatus actual = producerStatus(producer);
		assertEquals(expected, actual);
	}

	public static void assertStatuses(AbstractStreamConsumer.StreamConsumerStatus expected, List<? extends StreamConsumer<?>> streamConsumers) {
		AbstractStreamConsumer.StreamConsumerStatus[] statuses = consumerStatuses(streamConsumers);
		for (AbstractStreamConsumer.StreamConsumerStatus status : statuses) {
			assertEquals("Expected " + expected + ", actual: " + Arrays.toString(statuses), expected, status);
		}
	}

	public static void assertStatuses(AbstractStreamProducer.StreamProducerStatus expected, List<? extends StreamProducer<?>> streamProducers) {
		AbstractStreamProducer.StreamProducerStatus[] statuses = producerStatuses(streamProducers);
		for (AbstractStreamProducer.StreamProducerStatus status : statuses) {
			assertEquals("Expected " + expected + ", actual: " + Arrays.toString(statuses), expected, status);
		}
	}

}
