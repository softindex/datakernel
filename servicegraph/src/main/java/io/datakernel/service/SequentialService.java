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

public class SequentialService implements ConcurrentService {
	private static final Logger logger = LoggerFactory.getLogger(SequentialService.class);
	private final List<ConcurrentService> services;

	/**
	 * Initialize a new sequential service which is consisting services from argument
	 *
	 * @param services services for new sequential service
	 */
	public SequentialService(List<? extends ConcurrentService> services) {
		this.services = new ArrayList<>(services);
	}

	@Override
	public void start(ConcurrentServiceCallback callback) {
		doAction(services, new FunctionCallback<ConcurrentService>() {
			@Override
			public void apply(ConcurrentService input, ConcurrentServiceCallback callback) {
				input.start(callback);
			}

			@Override
			public String toString() {
				return "Starting";
			}
		}, callback);
	}

	@Override
	public void stop(ConcurrentServiceCallback callback) {
		doAction(reverse(services), new FunctionCallback<ConcurrentService>() {
			@Override
			public void apply(ConcurrentService input, ConcurrentServiceCallback callback) {
				input.stop(callback);
			}

			@Override
			public String toString() {
				return "Stopping";
			}
		}, callback);
	}

	private void doAction(Iterable<ConcurrentService> iterable, FunctionCallback<ConcurrentService> action, ConcurrentServiceCallback callback) {
		next(iterable.iterator(), action, callback);
	}

	private void next(final Iterator<ConcurrentService> it,
	                  final FunctionCallback<ConcurrentService> action,
	                  final ConcurrentServiceCallback callback) {
		while (it.hasNext()) {
			final ConcurrentService service = it.next();
			logger.info("{} {}", action, service);
			ConcurrentServiceCallback applyCallback = new ConcurrentServiceCallback() {
				@Override
				public void doOnComplete() {
					logger.info("{} sequential service {} complete", action, service);
					next(it, action, callback);
				}

				@Override
				public void doOnExeption(Exception e) {
					logger.error("Exception while {} {}", action, service);
					callback.onException(e);
					throw new RuntimeException(e);
				}
			};
			action.apply(service, applyCallback);
			return;
		}
		callback.onComplete();
	}

	private List<ConcurrentService> reverse(List<ConcurrentService> list) {
		ArrayList<ConcurrentService> concurrentServices = new ArrayList<>(list.size());
		for (int i = list.size() - 1; i >= 0; i--) {
			concurrentServices.add(list.get(i));
		}

		return concurrentServices;
	}
}
