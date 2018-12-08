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

import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.Executor;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.function.Supplier;

public interface EventloopExecutor extends Executor {
	CompletableFuture<Void> submit(Runnable computation);

	<T> CompletableFuture<T> submit(Callable<T> computation);

	default <T> CompletableFuture<T> submit(Supplier<CompletionStage<T>> computation) {
		return submit(cb -> computation.get().whenComplete(cb));
	}

	<T> CompletableFuture<T> submit(Consumer<BiConsumer<T, Throwable>> callbackConsumer);
}
