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

import io.datakernel.eventloop.Eventloop;
import io.datakernel.time.SteppingCurrentTimeProvider;
import org.junit.Test;

import java.util.concurrent.TimeoutException;

import static org.junit.Assert.assertTrue;

public class ResultCallbackWithTimeoutTest {
	@Test
	public void testTimeout() throws Exception {
		SteppingCurrentTimeProvider timeProvider = new SteppingCurrentTimeProvider(0, 1);
		Eventloop eventloop = new Eventloop(timeProvider);
		TestLoggingResultCallback<Integer> callback = new TestLoggingResultCallback<>();
		final ResultCallbackWithTimeout<Integer> callbackWithTimeout =
				new ResultCallbackWithTimeout<>(eventloop, callback, 10);
		eventloop.schedule(15, new Runnable() {
			@Override
			public void run() {
				callbackWithTimeout.onResult(42);
			}
		});

		eventloop.run();

		assertTrue(callback.results == 0);
		assertTrue(callback.exceptions == 1);
		assertTrue(callback.lastException instanceof TimeoutException);
	}

	@Test
	public void testResult() throws Exception {
		SteppingCurrentTimeProvider timeProvider = new SteppingCurrentTimeProvider(0, 1);
		Eventloop eventloop = new Eventloop(timeProvider);
		TestLoggingResultCallback<Integer> callback = new TestLoggingResultCallback<>();
		final ResultCallbackWithTimeout<Integer> callbackWithTimeout =
				new ResultCallbackWithTimeout<>(eventloop, callback, 10);

		eventloop.schedule(5, new Runnable() {
			@Override
			public void run() {
				callbackWithTimeout.onResult(42);
			}
		});

		eventloop.run();

		assertTrue(callback.results == 1);
		assertTrue(callback.exceptions == 0);
		assertTrue(callback.lastResult == 42);
	}

	@Test
	public void testException() throws Exception {
		SteppingCurrentTimeProvider timeProvider = new SteppingCurrentTimeProvider(0, 1);
		Eventloop eventloop = new Eventloop(timeProvider);
		TestLoggingResultCallback<Integer> callback = new TestLoggingResultCallback<>();
		final ResultCallbackWithTimeout<Integer> callbackWithTimeout =
				new ResultCallbackWithTimeout<>(eventloop, callback, 10);

		final Exception exception1 = new Exception("Exception1");
		eventloop.schedule(5, new Runnable() {
			@Override
			public void run() {
				callbackWithTimeout.onException(exception1);
			}
		});

		eventloop.run();

		assertTrue(callback.results == 0);
		assertTrue(callback.exceptions == 1);
		assertTrue(callback.lastException.equals(exception1));
	}

	@Test
	public void testWithCancellableCallback() throws Exception {
		SteppingCurrentTimeProvider timeProvider = new SteppingCurrentTimeProvider(0, 1);
		Eventloop eventloop = new Eventloop(timeProvider);
		TestLoggingCancellableCallback<Integer> callback = new TestLoggingCancellableCallback<>();
		final ResultCallbackWithTimeout<Integer> callbackWithTimeout =
				new ResultCallbackWithTimeout<>(eventloop, callback, 10);
		callback.cancel();

		eventloop.schedule(5, new Runnable() {
			@Override
			public void run() {
				callbackWithTimeout.onResult(42);
			}
		});

		eventloop.run();

		assertTrue(callback.results == 0);
		assertTrue(callback.exceptions == 0);
	}
}