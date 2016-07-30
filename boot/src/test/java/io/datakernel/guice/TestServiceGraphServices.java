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

package io.datakernel.guice;

import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import io.datakernel.service.Service;

public final class TestServiceGraphServices {
	private TestServiceGraphServices() {
	}

	public static Service immediateService() {
		return new Service() {
			@Override
			public ListenableFuture<?> start() {
				return Futures.immediateFuture(null);
			}

			@Override
			public ListenableFuture<?> stop() {
				return Futures.immediateFuture(null);
			}
		};
	}

	public static Service immediateFailedService(final Exception e) {
		return new Service() {
			@Override
			public ListenableFuture<?> start() {
				return Futures.immediateFailedFuture(e);
			}

			@Override
			public ListenableFuture<?> stop() {
				return Futures.immediateFailedFuture(e);
			}
		};
	}

}
