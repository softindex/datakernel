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

import static org.junit.Assert.assertEquals;

public class Utils {
	public static void assertStatus(StreamStatus expectedStatus, StreamProducer<?> streamProducer) {
		assertEquals(expectedStatus, ((AbstractStreamProducer<?>) streamProducer).getStatus());
	}

	public static void assertStatus(StreamStatus expectedStatus, StreamConsumer<?> streamProducer) {
		assertEquals(expectedStatus, ((AbstractStreamConsumer<?>) streamProducer).getStatus());
	}
}
