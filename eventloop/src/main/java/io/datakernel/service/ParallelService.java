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

import com.google.common.base.Function;
import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import org.slf4j.Logger;

import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicInteger;

import static com.google.common.util.concurrent.MoreExecutors.sameThreadExecutor;
import static org.slf4j.LoggerFactory.getLogger;

/**
 * ParallelService  processes services in parallel, in no specific sequence. It is implementation
 * of the {@link ConcurrentService}
 */
public final class ParallelService implements ConcurrentService {
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

	@SuppressWarnings("ConstantConditions")
	private ListenableFuture<?> doAction(final Function<ConcurrentService, ListenableFuture<?>> action) {
		if (services.isEmpty())
			return Futures.immediateFuture(true);
		final SettableFuture<Boolean> future = SettableFuture.create();
		final AtomicInteger counter = new AtomicInteger(services.size());
		for (final ConcurrentService service : services) {
			final ListenableFuture<?> serviceFuture = action.apply(service);
			if (serviceFuture.isDone()) {
				try {
					serviceFuture.get();
					logger.info("{} {} complete", action, service);
				} catch (InterruptedException | ExecutionException e) {
					future.setException(e);
					logger.error("Exception while {} {}", action, service);
					break;
				}

				if (counter.decrementAndGet() == 0) {
					future.set(true);
				}
			} else {
				serviceFuture.addListener(new Runnable() {
					@Override
					public void run() {
						try {
							serviceFuture.get();
							logger.info("{} {} complete", action, service);
						} catch (InterruptedException | ExecutionException e) {
							future.setException(e);
							logger.error("Exception while {} {}", action, service);
							return;
						}

						if (counter.decrementAndGet() == 0) {
							future.set(true);
						}
					}
				}, sameThreadExecutor());
			}
		}
		return future;
	}

	/**
	 * Starts all asynchronous services
	 *
	 * @return ListenableFuture with listener which guaranteed to be called once the action is
	 * complete.It is used as an input to another derived Future
	 */
	@Override
	public ListenableFuture<?> startFuture() {
		return doAction(new Function<ConcurrentService, ListenableFuture<?>>() {
			@Override
			public ListenableFuture<?> apply(ConcurrentService input) {
				return input.startFuture();
			}

			@Override
			public String toString() {
				return "Starting";
			}
		});
	}

	/**
	 * Stops all asynchronous services
	 *
	 * @return ListenableFuture with listener which guaranteed to be called once the action is
	 * complete.It is used as an input to another derived Future
	 */
	@Override
	public ListenableFuture<?> stopFuture() {
		return doAction(new Function<ConcurrentService, ListenableFuture<?>>() {
			@Override
			public ListenableFuture<?> apply(ConcurrentService input) {
				return input.stopFuture();
			}

			@Override
			public String toString() {
				return "Stopping";
			}
		});
	}
}
