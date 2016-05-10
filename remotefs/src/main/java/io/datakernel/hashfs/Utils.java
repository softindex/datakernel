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

package io.datakernel.hashfs;

import io.datakernel.async.ResultCallback;

import java.util.ArrayList;
import java.util.List;

final class Utils {
	static <T> ResultCallback<T> waitAllResults(final int count, final Resolver<T> resolver) {
		return new ResultCallback<T>() {
			List<T> results = new ArrayList<>();
			List<Exception> exceptions = new ArrayList<>();
			int completed = 0;
			int failed = 0;

			@Override
			public void onResult(T result) {
				completed++;
				results.add(result);
				onCompleteOrException();
			}

			@Override
			public void onException(Exception e) {
				failed++;
				exceptions.add(e);
				onCompleteOrException();
			}

			private void onCompleteOrException() {
				if (completed + failed == count) {
					resolver.resolve(results, exceptions);
				}
			}
		};
	}

	interface Resolver<T> {
		void resolve(List<T> results, List<Exception> e);
	}
}