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

import io.datakernel.async.AsyncCallable;
import io.datakernel.async.AsyncRunnable;

import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;

public interface EventloopExecutor extends Executor {
	CompletableFuture<Void> submit(Runnable runnable);

	CompletableFuture<Void> submit(AsyncRunnable asyncRunnable);

	<T> CompletableFuture<T> submit(Runnable runnable, T result);

	<T> CompletableFuture<T> submit(AsyncRunnable asyncRunnable, T result);

	<T> CompletableFuture<T> submit(Callable<T> callable);

	<T> CompletableFuture<T> submit(AsyncCallable<T> asyncCallable);
}
