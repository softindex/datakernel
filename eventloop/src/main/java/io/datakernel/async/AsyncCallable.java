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

import static io.datakernel.async.AsyncCallbacks.throwableToException;

public interface AsyncCallable<T> {
	default CompletionStage<T> call() {
		SettableStage<T> stage = SettableStage.create();
		call(new ResultCallback<T>() {
			@Override
			protected void onResult(T result) {
				stage.setResult(result);
			}

			@Override
			protected void onException(Exception e) {
				stage.setError(e);
			}
		});
		return stage;
	}

	default void call(ResultCallback<T> callback) {
		call().whenComplete((t, throwable) -> {
			if (throwable == null) {
				callback.setResult(t);
			} else {
				callback.setException(throwableToException(throwable));
			}
		});
	}
}