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
import java.util.Iterator;
import java.util.List;

public final class SequentialService implements AsyncService {
	private static final Logger logger = LoggerFactory.getLogger(SequentialService.class);
	private final List<AsyncService> services;

	/**
	 * Initialize a new sequential service which is consisting services from argument
	 *
	 * @param services services for new sequential service
	 */
	public SequentialService(List<? extends AsyncService> services) {
		this.services = new ArrayList<>(services);
	}

	@Override
	public void start(AsyncServiceCallback callback) {
		doAction(services, new FunctionCallback<AsyncService>() {
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
	public void stop(AsyncServiceCallback callback) {
		doAction(reverse(services), new FunctionCallback<AsyncService>() {
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

	private void doAction(Iterable<AsyncService> iterable, FunctionCallback<AsyncService> action, AsyncServiceCallback callback) {
		next(iterable.iterator(), action, callback);
	}

	private void next(final Iterator<AsyncService> it,
	                  final FunctionCallback<AsyncService> action,
	                  final AsyncServiceCallback callback) {
		while (it.hasNext()) {
			final AsyncService service = it.next();
			logger.info("{} {}", action, service);
			AsyncServiceCallback applyCallback = new AsyncServiceCallback() {
				@Override
				public void onComplete() {
					logger.info("{} sequential service {} complete", action, service);
					next(it, action, callback);
				}

				@Override
				public void onException(Exception e) {
					logger.error("Exception while {} {}", action, service);
					callback.onException(e);
				}
			};
			action.apply(service, applyCallback);
			return;
		}
		callback.onComplete();
	}

	private List<AsyncService> reverse(List<AsyncService> list) {
		ArrayList<AsyncService> asyncServices = new ArrayList<>(list.size());
		for (int i = list.size() - 1; i >= 0; i--) {
			asyncServices.add(list.get(i));
		}

		return asyncServices;
	}
}
