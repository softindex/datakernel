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

public class ConcurrentServices {
	private ConcurrentServices() {
	}

	public static ConcurrentService immediateService() {
		return new ConcurrentService() {
			@Override
			public void start(ConcurrentServiceCallback callback) {
				callback.onComplete();
			}

			@Override
			public void stop(ConcurrentServiceCallback callback) {
				callback.onComplete();
			}
		};
	}

	public static ConcurrentService immediateFailedService(final Exception e) {
		return new ConcurrentService() {
			@Override
			public void start(ConcurrentServiceCallback callback) {
				callback.onException(e);
			}

			@Override
			public void stop(ConcurrentServiceCallback callback) {
				callback.onComplete();
			}
		};
	}

	public static ConcurrentService parallelService(ConcurrentService... callbacks) {
		return new ParallelService(Arrays.asList(callbacks));
	}

	public static ConcurrentService parallelService(List<? extends ConcurrentService> callbacks) {
		return new ParallelService(callbacks);
	}

	public static ConcurrentService sequentialService(ConcurrentService... callbacks) {
		return new SequentialService(Arrays.asList(callbacks));
	}

	public static ConcurrentService sequentialService(List<? extends ConcurrentService> callbacks) {
		return new SequentialService(callbacks);
	}
}
