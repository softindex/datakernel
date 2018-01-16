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

package io.datakernel.stream;

import static org.junit.Assert.assertEquals;

public class TestUtils {

	public static void assertStatus(StreamStatus expectedStatus, StreamProducer<?> streamProducer) {
		if (streamProducer instanceof StreamProducerDecorator) {
			assertStatus(expectedStatus, ((StreamProducerDecorator) streamProducer).getActualProducer());
			return;
		}
		if (expectedStatus == StreamStatus.CLOSED_WITH_ERROR && streamProducer instanceof StreamProducers.ClosingWithErrorImpl)
			return;
		assertEquals(expectedStatus, ((AbstractStreamProducer<?>) streamProducer).getStatus());
	}

	public static void assertStatus(StreamStatus expectedStatus, StreamConsumer<?> streamConsumer) {
		if (streamConsumer instanceof StreamConsumerDecorator) {
			assertStatus(expectedStatus, ((StreamConsumerDecorator) streamConsumer).getActualConsumer());
			return;
		}
		if (expectedStatus == StreamStatus.CLOSED_WITH_ERROR && streamConsumer instanceof StreamConsumers.ClosingWithErrorImpl)
			return;
		assertEquals(expectedStatus, ((AbstractStreamConsumer<?>) streamConsumer).getStatus());
	}
}
