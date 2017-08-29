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

package io.datakernel.async;

import java.util.concurrent.CompletionStage;

import static io.datakernel.async.AsyncCallbacks.completionToStage;
import static io.datakernel.async.AsyncCallbacks.throwableToException;

/**
 * This interface represents asynchronous task
 */
public interface AsyncRunnable {
	/**
	 * Executes this asynchronous task with callback from argument
	 */
	default void run(CompletionCallback callback) {
		run().whenComplete(($, throwable) -> {
			if (throwable != null)
				callback.setException(throwableToException(throwable));
			else
				callback.setComplete();
		});
	}

	default CompletionStage<Void> run() {
		SettableStage<Void> stage = SettableStage.create();
		run(completionToStage(stage));
		return stage;
	}
}
