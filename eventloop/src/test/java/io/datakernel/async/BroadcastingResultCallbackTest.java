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

package io.datakernel.async;

import org.junit.Test;

import java.util.Arrays;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class BroadcastingResultCallbackTest {
	@Test
	public void testSimpleCallbacksResult() throws Exception {
		TestLoggingResultCallback<Integer> callback1 = new TestLoggingResultCallback<>();
		TestLoggingResultCallback<Integer> callback2 = new TestLoggingResultCallback<>();
		TestLoggingResultCallback<Integer> callback3 = new TestLoggingResultCallback<>();
		BroadcastingResultCallback<Integer> broadcastingCallback =
				new BroadcastingResultCallback<>(Arrays.asList((ResultCallback<Integer>) callback1, callback2, callback3));

		broadcastingCallback.onResult(42);
		broadcastingCallback.onResult(11);

		assertTrue(callback1.results == 1);
		assertTrue(callback2.results == 1);
		assertTrue(callback3.results == 1);
		assertTrue(callback1.exceptions == 0);
		assertTrue(callback2.exceptions == 0);
		assertTrue(callback3.exceptions == 0);
		assertTrue(callback1.lastResult == 42);
		assertTrue(callback2.lastResult == 42);
		assertTrue(callback3.lastResult == 42);
	}

	@Test
	public void testSimpleCallbacksException() throws Exception {
		TestLoggingResultCallback<Integer> callback1 = new TestLoggingResultCallback<>();
		TestLoggingResultCallback<Integer> callback2 = new TestLoggingResultCallback<>();
		TestLoggingResultCallback<Integer> callback3 = new TestLoggingResultCallback<>();
		BroadcastingResultCallback<Integer> broadcastingCallback =
				new BroadcastingResultCallback<>(Arrays.asList((ResultCallback<Integer>) callback1, callback2, callback3));

		Exception exception1 = new Exception("Exception1");
		Exception exception2 = new Exception("Exception2");
		broadcastingCallback.onException(exception1);
		broadcastingCallback.onException(exception2);

		assertTrue(callback1.results == 0);
		assertTrue(callback2.results == 0);
		assertTrue(callback3.results == 0);
		assertTrue(callback1.exceptions == 1);
		assertTrue(callback2.exceptions == 1);
		assertTrue(callback3.exceptions == 1);
		assertTrue(callback1.lastException.equals(exception1));
		assertTrue(callback2.lastException.equals(exception1));
		assertTrue(callback3.lastException.equals(exception1));
	}

	@Test
	public void testAllCancelledCallbacks() throws Exception {
		TestLoggingCancellableCallback<Integer> callback1 = new TestLoggingCancellableCallback<>();
		callback1.cancel();
		TestLoggingCancellableCallback<Integer> callback2 = new TestLoggingCancellableCallback<>();
		callback2.cancel();
		TestLoggingCancellableCallback<Integer> callback3 = new TestLoggingCancellableCallback<>();
		callback3.cancel();

		BroadcastingResultCallback<Integer> broadcastingCallback =
				new BroadcastingResultCallback<>(Arrays.asList((ResultCallback<Integer>) callback1, callback2, callback3));

		assertTrue(broadcastingCallback.isCancelled());

		broadcastingCallback.onResult(42);

		assertTrue(callback1.results == 0);
	}

	@Test
	public void testCancellableCallbacks() throws Exception {
		TestLoggingCancellableCallback<Integer> callback1 = new TestLoggingCancellableCallback<>();
		TestLoggingCancellableCallback<Integer> callback2 = new TestLoggingCancellableCallback<>();
		TestLoggingCancellableCallback<Integer> callback3 = new TestLoggingCancellableCallback<>();

		BroadcastingResultCallback<Integer> broadcastingCallback =
				new BroadcastingResultCallback<>(Arrays.asList((ResultCallback<Integer>) callback1, callback2, callback3));

		callback2.cancel();

		assertFalse(broadcastingCallback.isCancelled());

		broadcastingCallback.onResult(42);

		assertTrue(callback1.results == 1);
		assertTrue(callback2.results == 0);
		assertTrue(callback3.results == 1);
	}
}