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

package io.datakernel.service;

import java.util.Arrays;
import java.util.List;

public class AsyncServices {
	private AsyncServices() {
	}

	public static AsyncService immediateService() {
		return new AsyncService() {
			@Override
			public void start(AsyncServiceCallback callback) {
				callback.onComplete();
			}

			@Override
			public void stop(AsyncServiceCallback callback) {
				callback.onComplete();
			}
		};
	}

	public static AsyncService immediateFailedService(final Exception e) {
		return new AsyncService() {
			@Override
			public void start(AsyncServiceCallback callback) {
				callback.onException(e);
			}

			@Override
			public void stop(AsyncServiceCallback callback) {
				callback.onException(e);
			}
		};
	}

	public static AsyncService parallelService(AsyncService... callbacks) {
		return new ParallelService(Arrays.asList(callbacks));
	}

	public static AsyncService parallelService(List<? extends AsyncService> callbacks) {
		return new ParallelService(callbacks);
	}

	public static AsyncService sequentialService(AsyncService... callbacks) {
		return new SequentialService(Arrays.asList(callbacks));
	}

	public static AsyncService sequentialService(List<? extends AsyncService> callbacks) {
		return new SequentialService(callbacks);
	}
}
