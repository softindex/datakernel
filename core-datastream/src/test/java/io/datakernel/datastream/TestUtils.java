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

package io.datakernel.datastream;

import java.util.List;

import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;

public class TestUtils {
	public static void assertEndOfStream(StreamSupplier<?> streamSupplier) {
		assertTrue(streamSupplier.getEndOfStream().isResult());
	}

	public static void assertEndOfStream(StreamConsumer<?> streamConsumer) {
		assertTrue(streamConsumer.getAcknowledgement().isResult());
	}

	public static void assertEndOfStream(StreamSupplier<?> streamSupplier, StreamConsumer<?> streamConsumer) {
		assertEndOfStream(streamSupplier);
		assertEndOfStream(streamConsumer);
	}

	public static void assertClosedWithError(StreamSupplier<?> streamSupplier) {
		assertTrue(streamSupplier.getEndOfStream().isException());
	}

	public static void assertClosedWithError(Throwable throwable, StreamSupplier<?> streamSupplier) {
		assertSame(throwable, streamSupplier.getEndOfStream().getException());
	}

	public static void assertClosedWithError(StreamConsumer<?> streamConsumer) {
		assertTrue(streamConsumer.getAcknowledgement().isException());
	}

	public static void assertClosedWithError(Throwable throwable, StreamConsumer<?> streamConsumer) {
		assertSame(throwable, streamConsumer.getAcknowledgement().getException());
	}

	public static void assertClosedWithError(Throwable throwable, StreamSupplier<?> streamSupplier, StreamConsumer<?> streamConsumer) {
		assertSame(throwable, streamSupplier.getEndOfStream().getException());
		assertSame(throwable, streamConsumer.getAcknowledgement().getException());
	}

	public static void assertSuppliersEndOfStream(List<? extends StreamSupplier<?>> streamSuppliers) {
		assertTrue(streamSuppliers.stream().allMatch(v -> v.getEndOfStream().isResult()));
	}

	public static void assertConsumersEndOfStream(List<? extends StreamConsumer<?>> streamConsumers) {
		assertTrue(streamConsumers.stream().allMatch(v -> v.getAcknowledgement().isResult()));
	}

	public static void assertSuppliersClosedWithError(List<? extends StreamSupplier<?>> streamSuppliers) {
		assertTrue(streamSuppliers.stream().allMatch(v -> v.getEndOfStream().isException()));
	}

	public static void assertConsumersClosedWithError(List<? extends StreamConsumer<?>> streamConsumers) {
		assertTrue(streamConsumers.stream().allMatch(v -> v.getAcknowledgement().isException()));
	}
}
