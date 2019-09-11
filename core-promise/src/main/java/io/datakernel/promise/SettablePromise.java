/*
 * Copyright (C) 2015-2018 SoftIndex LLC.
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

package io.datakernel.promise;

import io.datakernel.async.callback.Callback;
import org.jetbrains.annotations.Async;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import static io.datakernel.eventloop.Eventloop.getCurrentEventloop;

/**
 * Represents a {@link Promise} which can be completed or completedExceptionally
 * manually at once or later in the future.
 * <p>
 * Can be used as root {@code Promise} to start execution of chain of
 * {@code Promises} or when you want wrap your actions in {@code Promise}.
 *
 * @param <T> result type
 */
public final class SettablePromise<T> extends AbstractPromise<T> implements Callback<T> {
	/**
	 * Accepts the provided values and performs this operation
	 * on them. If the {@code Throwable e} is {@code null},
	 * provided {@code result} will be set to this
	 * {@code SettablePromise}.
	 * <p>
	 * Otherwise, {@code Throwable e} will be set.
	 *
	 * @param result a value to be set to this
	 * 				{@code SettablePromise} if
	 * 			    {@code e} is {@code null}
	 * @param e 	a {@code Throwable}, which will
	 *          	be set to this {@code SettablePromise}
	 *          	if not {@code null}
	 */
	@Override
	public void accept(T result, @Nullable Throwable e) {
		if (e == null) {
			set(result);
		} else {
			setException(e);
		}
	}

	/**
	 * Sets the result of this {@code SettablePromise} and
	 * completes it. {@code AssertionError} is thrown when you
	 * try to set result for an already completed {@code Promise}.
	 */
	@Async.Execute
	public void set(T result) {
		complete(result);
	}

	/**
	 * Sets exception and completes this {@code SettablePromise} exceptionally.
	 * {@code AssertionError} is thrown when you try to set exception for
	 * an already completed {@code Promise}.
	 *
	 * @param e exception
	 */
	@Async.Execute
	public void setException(@NotNull Throwable e) {
		completeExceptionally(e);
	}

	/**
	 * Tries to set provided {@code result} for this
	 * {@code SettablePromise} if it is not completed yet.
	 * Otherwise does nothing.
	 */
	@Async.Execute
	public void trySet(T result) {
		if (!isComplete()) {
			set(result);
		}
	}

	/**
	 * Tries to set provided {@code e} exception for this
	 * {@code SettablePromise} if it is not completed yet.
	 * Otherwise does nothing.
	 */
	@Async.Execute
	public void trySetException(@NotNull Throwable e) {
		if (!isComplete()) {
			setException(e);
		}
	}

	/**
	 * Tries to set result or exception for this {@code SettablePromise}
	 * if it not completed yet. Otherwise does nothing.
	 */
	@Async.Execute
	public void trySet(T result, @Nullable Throwable e) {
		if (!isComplete()) {
			if (e == null) {
				trySet(result);
			} else {
				trySetException(e);
			}
		}
	}

	public void post(T result) {
		getCurrentEventloop().post(() -> set(result));
	}

	public void postException(@NotNull Throwable e) {
		getCurrentEventloop().post(() -> setException(e));
	}

	public void post(T result, @Nullable Throwable e) {
		getCurrentEventloop().post(() -> accept(result, e));
	}

	public void tryPost(T result) {
		getCurrentEventloop().post(() -> trySet(result));
	}

	public void tryPostException(@NotNull Throwable e) {
		getCurrentEventloop().post(() -> trySetException(e));
	}

	public void tryPost(T result, @Nullable Throwable e) {
		getCurrentEventloop().post(() -> trySet(result, e));
	}

	@Override
	public String toString() {
		return "SettablePromise{" +
				(isComplete() ?
						(exception == null ?
								"" + result :
								"exception=" + exception.getClass().getSimpleName()) :
						"<unset>")
				+ "}";
	}
}
