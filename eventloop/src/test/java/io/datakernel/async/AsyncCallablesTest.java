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
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

public class AsyncCallablesTest {
	@Test
	public void test() {
		Eventloop eventloop = Eventloop.create();

		AsyncCallable<String> callable1 = new AsyncCallable<String>() {
			@Override
			public void call(ResultCallback<String> callback) {
				callback.setResult("1");
			}
		};
		AsyncCallable<String> callable2 = new AsyncCallable<String>() {
			@Override
			public void call(ResultCallback<String> callback) {
				callback.setResult("2");
			}
		};

		List<AsyncCallable<String>> callables = new ArrayList<>();
		callables.add(callable1);
		callables.add(callable2);

		AsyncCallable<List<String>> timeoutCallable = AsyncCallables.callAllWithTimeout(eventloop, 12345, callables);
		timeoutCallable.call(new AssertingResultCallback<List<String>>() {
			@Override
			protected void onResult(List<String> results) {
			}
		});

		eventloop.run();
	}

}