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

package io.datakernel.async.service;

import io.datakernel.eventloop.Eventloop;
import io.datakernel.promise.Promise;
import org.jetbrains.annotations.NotNull;

import java.util.concurrent.CompletableFuture;

/**
 * This interface is for services that can be started and then stopped
 * in the context of eventloop, so it works with {@link Promise}
 */
public interface EventloopService {
	@NotNull
	Eventloop getEventloop();

	/**
	 * Starts this component asynchronously.
	 * Callback completes immediately if the component is already running.
	 */
	@NotNull
	Promise<?> start();

	@NotNull
	default CompletableFuture<?> startFuture() {
		return getEventloop().submit(this::start);
	}

	/**
	 * Stops this component asynchronously.
	 * Callback completes immediately if the component is not running / already stopped.
	 */
	@NotNull
	Promise<?> stop();

	@NotNull
	default CompletableFuture<?> stopFuture() {
		return getEventloop().submit(this::stop);
	}
}
