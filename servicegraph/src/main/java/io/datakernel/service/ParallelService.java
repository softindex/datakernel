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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

public class ParallelService implements AsyncService {
	private static final Logger logger = LoggerFactory.getLogger(ParallelService.class);
	private final List<AsyncService> services;

	/**
	 * Initialize a new parallel service, which is consisting services from argument
	 *
	 * @param services services for new parallel service
	 */
	public ParallelService(List<? extends AsyncService> services) {
		this.services = new ArrayList<>(services);
	}

	@Override
	public void start(AsyncServiceCallback callback) {
		doAction(new FunctionCallback<AsyncService>() {

			@Override
			public void apply(AsyncService input, AsyncServiceCallback callback) {
				input.start(callback);
			}

			@Override
			public String toString() {
				return "Starting";
			}
		}, callback);
	}

	@Override
	public void stop(final AsyncServiceCallback callback) {
		doAction(new FunctionCallback<AsyncService>() {
			@Override
			public void apply(AsyncService input, AsyncServiceCallback callback) {
				input.stop(callback);
			}

			@Override
			public String toString() {
				return "Stopping";
			}
		}, callback);
	}

	@SuppressWarnings("ConstantConditions")
	private void doAction(final FunctionCallback<AsyncService> action, final AsyncServiceCallback callback) {
		if (services.isEmpty()) {
			callback.onComplete();
			return;
		}
		final AtomicInteger counter = new AtomicInteger(services.size());
		for (final AsyncService service : services) {
			AsyncServiceCallback applyCallback = new AsyncServiceCallback() {
				@Override
				public void onComplete() {
					logger.info("{} {} complete", action, service);
					if (counter.decrementAndGet() == 0) {
						callback.onComplete();
					}
				}

				@Override
				public void onException(Exception e) {
					logger.error("Exception while {} {}", action, service);
					callback.onException(e);
				}
			};
			action.apply(service, applyCallback);
		}
	}
}
