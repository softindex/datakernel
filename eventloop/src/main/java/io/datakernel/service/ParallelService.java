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

import com.google.common.collect.ImmutableList;
import org.slf4j.Logger;

import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import static com.google.common.base.Throwables.propagate;
import static com.google.common.util.concurrent.MoreExecutors.sameThreadExecutor;
import static org.slf4j.LoggerFactory.getLogger;

public class ParallelService implements ConcurrentService {
	private static final Logger logger = getLogger(ParallelService.class);
	private final List<ConcurrentService> services;

	/**
	 * Initialize a new parallel service, which is consisting services from argument
	 *
	 * @param services services for new parallel service
	 */
	public ParallelService(List<? extends ConcurrentService> services) {
		this.services = ImmutableList.copyOf(services);
	}


	@Override
	public void startFuture(SimpleCompletionFuture callback) {
		doAction(new FunctionCallback<ConcurrentService>() {

			@Override
			public void apply(ConcurrentService input, SimpleCompletionFuture callback) {
				input.startFuture(callback);
			}

			@Override
			public String toString() {
				return "Starting";
			}
		}, callback);
	}

	@Override
	public void stopFuture(final SimpleCompletionFuture callback) {
		doAction(new FunctionCallback<ConcurrentService>() {
			@Override
			public void apply(ConcurrentService input, SimpleCompletionFuture callback) {
				input.stopFuture(callback);
			}

			@Override
			public String toString() {
				return "Stopping";
			}
		}, callback);
	}

	@SuppressWarnings("ConstantConditions")
	private void doAction(final FunctionCallback<ConcurrentService> action, final SimpleCompletionFuture callback) {
		if (services.isEmpty()) {
			callback.onSuccess();
			return;
		}
		final AtomicInteger counter = new AtomicInteger(services.size());
		for (final ConcurrentService service : services) {
			final Boolean[] breakCallback = {Boolean.FALSE};
			SimpleCompletionFuture applyCallback = new SimpleCompletionFuture() {
				@Override
				public void doOnSuccess() {
					logger.info("{} {} complete", action, service);
					if (counter.decrementAndGet() == 0) {
						sameThreadExecutor().execute(new Runnable() {
							@Override
							public void run() {
								callback.onSuccess();
							}
						});
					}
				}

				@Override
				public void doOnError(Exception e) {
					logger.error("Exception while {} {}", action, service);
					propagate(e);
					breakCallback[0] = Boolean.TRUE;

					if (counter.decrementAndGet() == 0) {
						sameThreadExecutor().execute(new Runnable() {
							@Override
							public void run() {
								callback.onSuccess();
							}
						});
					}
				}
			};
			action.apply(service, applyCallback);
			if (breakCallback[0]) {
				break;
			}

		}
	}
}
