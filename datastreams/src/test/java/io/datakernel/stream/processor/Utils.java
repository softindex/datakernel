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
			result[i] = ((AbstractStreamProducer<?>)streamProducer).getStatus();
		}
		return result;
	}

	public static StreamStatus[] consumerStatuses(List<? extends StreamConsumer<?>> streamConsumers) {
		StreamStatus[] result = new StreamStatus[streamConsumers.size()];
		for (int i = 0; i < streamConsumers.size(); i++) {
			StreamConsumer<?> streamConsumer = streamConsumers.get(i);
			result[i] = ((AbstractStreamConsumer<?>)streamConsumer).getStatus();
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

	public static void assertStatus(StreamStatus expectedStatus, StreamProducer<?> streamProducer) {
		assertEquals(expectedStatus, ((AbstractStreamProducer<?>) streamProducer).getStatus());
	}

	public static void assertStatus(StreamStatus expectedStatus, StreamConsumer<?> streamProducer) {
		assertEquals(expectedStatus, ((AbstractStreamConsumer<?>) streamProducer).getStatus());
	}



}
