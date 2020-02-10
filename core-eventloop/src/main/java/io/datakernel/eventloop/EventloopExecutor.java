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

package io.datakernel.eventloop;

import io.datakernel.async.callback.Completable;
import org.jetbrains.annotations.NotNull;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.function.Supplier;

/**
 * An abstraction over the {@link Eventloop} that can receive, dispatch and run tasks in it.
 * <p>
 * As a plain case, {@link Eventloop} itself implements it and posts received tasks to its own queues.
 *
 * @see BlockingEventloopExecutor
 */
public interface EventloopExecutor extends Executor {
	/**
	 * Executes the given computation at some time in the future in some undelying eventloop.
	 */
	@NotNull CompletableFuture<Void> submit(@NotNull Runnable computation);

	/**
	 * Executes the given computation at some time in the future in some undelying eventloop
	 * and returns its result in a {@link CompletableFuture future}.
	 */
	@NotNull <T> CompletableFuture<T> submit(Supplier<? extends Completable<T>> computation);
}
