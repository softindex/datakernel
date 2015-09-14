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

import java.util.Iterator;
import java.util.List;

import static com.google.common.base.Throwables.propagate;
import static com.google.common.collect.Lists.reverse;
import static com.google.common.util.concurrent.MoreExecutors.sameThreadExecutor;
import static org.slf4j.LoggerFactory.getLogger;

public class SequentialService implements ConcurrentService {
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

	@Override
	public void startFuture(SimpleCompletionFuture callback) {
		doAction(services, new FunctionCallback<ConcurrentService>() {
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
	public void stopFuture(SimpleCompletionFuture callback) {
		doAction(reverse(services), new FunctionCallback<ConcurrentService>() {
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

	private void doAction(Iterable<ConcurrentService> iterable, FunctionCallback<ConcurrentService> action, SimpleCompletionFuture callback) {
		next(iterable.iterator(), action, callback);
	}

	private void next(final Iterator<ConcurrentService> it,
	                  final FunctionCallback<ConcurrentService> action,
	                  final SimpleCompletionFuture callback) {
		while (it.hasNext()) {
			final ConcurrentService service = it.next();
			logger.info("{} {}", action, service);
			SimpleCompletionFuture applyCallback = new SimpleCompletionFuture() {
				@Override
				public void doOnSuccess() {
					logger.info("{} {} complete", action, service);
					next(it, action, callback);
				}

				@Override
				public void doOnError(Exception e) {
					logger.error("Exception while {} {}", action, service);
					callback.onError(e);
					propagate(e);
				}
			};
			action.apply(service, applyCallback);
			return;
		}
		sameThreadExecutor().execute(new Runnable() {
			@Override
			public void run() {
				callback.onSuccess();
			}
		});
	}
}
