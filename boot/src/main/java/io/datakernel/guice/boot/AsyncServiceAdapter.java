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

package io.datakernel.guice.boot;

import io.datakernel.service.AsyncService;

import java.util.concurrent.Executor;

/**
 * Adapter which Creates a new ConcurrentServices from other instances for working with ServiceGraph
 *
 * @param <V> type of service from which you need create ConcurrentService
 */
public interface AsyncServiceAdapter<V> {
	/**
	 * Wraps instances so that it will be ConcurrentService.
	 *
	 * @param node     instance from which will be created ConcurrentService
	 * @param executor object that executes submitted Runnable tasks
	 * @return new instance of  ConcurrentService
	 */
	AsyncService toService(V node, Executor executor);
}
