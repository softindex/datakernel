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

import static org.junit.Assert.assertTrue;

public class ListenableResultCallbackTest {
	@Test
	public void testSimpleCallbacksResult() throws Exception {
		TestLoggingResultCallback<Integer> callback1 = new TestLoggingResultCallback<>();
		TestLoggingResultCallback<Integer> callback2 = new TestLoggingResultCallback<>();
		TestLoggingResultCallback<Integer> callback3 = new TestLoggingResultCallback<>();
		ListenableResultCallback<Integer> listenableCallback = ListenableResultCallback.create();

		listenableCallback.addListener(callback1);
		listenableCallback.addListener(callback2);
		listenableCallback.addListener(callback3);

		listenableCallback.onResult(42);
		listenableCallback.onResult(11);

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
		ListenableResultCallback<Integer> listenableCallback = ListenableResultCallback.create();

		listenableCallback.addListener(callback1);
		listenableCallback.addListener(callback2);
		listenableCallback.addListener(callback3);

		Exception exception1 = new Exception("Exception1");
		Exception exception2 = new Exception("Exception2");
		listenableCallback.onException(exception1);
		listenableCallback.onException(exception2);

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
	public void testImmediatelyAvailableResult() throws Exception {
		TestLoggingResultCallback<Integer> simpleCallback1 = new TestLoggingResultCallback<>();
		TestLoggingResultCallback<Integer> simpleCallback2 = new TestLoggingResultCallback<>();
		ListenableResultCallback<Integer> listenableCallback1 = ListenableResultCallback.create();
		ListenableResultCallback<Integer> listenableCallback2 = ListenableResultCallback.create();

		listenableCallback1.onResult(42);
		Exception exception1 = new Exception("Exception1");
		listenableCallback2.onException(exception1);

		listenableCallback1.addListener(simpleCallback1);
		listenableCallback2.addListener(simpleCallback2);

		assertTrue(simpleCallback1.lastResult == 42);
		assertTrue(simpleCallback2.lastException.equals(exception1));
	}

}