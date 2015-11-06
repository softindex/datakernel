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
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import org.slf4j.Logger;

import java.util.Iterator;
import java.util.List;
import java.util.concurrent.ExecutionException;

import static com.google.common.collect.Lists.reverse;
import static com.google.common.util.concurrent.MoreExecutors.sameThreadExecutor;
import static org.slf4j.LoggerFactory.getLogger;

/**
 * SequentialService  processes services in  the order received. You can control the order
 * in which services are processed. It is implementation of {@link ConcurrentService}.
 */
public final class SequentialService implements ConcurrentService {
	private static final Logger logger = getLogger(SequentialService.class);
	private final List<ConcurrentService> services;

	/**
	 * Initialize a new sequential service which is consisting services from argument
	 *
	 * @param services services for new sequential service
	 */
	public SequentialService(List<? extends ConcurrentService> services) {
		this.services = ImmutableList.copyOf(services);
	}

	@SuppressWarnings("ConstantConditions")
	private void next(final SettableFuture<Boolean> future, final Iterator<ConcurrentService> it,
	                  final Function<ConcurrentService, ListenableFuture<?>> action) {
		while (it.hasNext()) {
			final ConcurrentService service = it.next();
			logger.info("{} {}", action, service);
			final ListenableFuture<?> serviceFuture = action.apply(service);
			if (serviceFuture.isDone()) {
				try {
					serviceFuture.get();
					logger.info("{} {} complete", action, service);
				} catch (InterruptedException | ExecutionException e) {
					future.setException(e);
					logger.error("Exception while {} {}", action, service);
					return;
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
						next(future, it, action);
					}
				}, sameThreadExecutor());
				return;
			}
		}
		future.set(true);
	}

	private ListenableFuture<?> doAction(Iterable<ConcurrentService> iterable, Function<ConcurrentService, ListenableFuture<?>> action) {
		SettableFuture<Boolean> future = SettableFuture.create();
		next(future, iterable.iterator(), action);
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
		return doAction(services, new Function<ConcurrentService, ListenableFuture<?>>() {
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
		return doAction(reverse(services), new Function<ConcurrentService, ListenableFuture<?>>() {
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