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

package io.datakernel.async.process;

import io.datakernel.common.exception.CloseException;
import org.jetbrains.annotations.NotNull;

/**
 * Describes methods that are used to handle exceptional behaviour or to handle closing.
 * <p>
 * After {@link #close()}, {@link #close(Throwable)} or {@link #cancel()} is called, the following things
 * should be done:
 * <ul>
 * <li>Resources held by an object should be freed</li>
 * <li>All pending asynchronous operations should be completed exceptionally</li>
 * </ul>
 * All operations of this interface are idempotent.
 */
public interface Cancellable {
	CloseException CANCEL_EXCEPTION = new CloseException(Cancellable.class, "Cancelled");
	CloseException CLOSE_EXCEPTION = new CloseException(Cancellable.class, "Closed");

	/**
	 * Closes process exceptionally in case an exception
	 * is thrown while executing the given process.
	 *
	 * @param e exception that is used to close process with
	 */
	void close(@NotNull Throwable e);

	/**
	 * Cancels the process.
	 */
	default void cancel() {
		close(CANCEL_EXCEPTION);
	}

	/**
	 * Closes process when it has finished.
	 */
	default void close() {
		close(CLOSE_EXCEPTION);
	}

	static void tryCancel(Object item) {
		if (item instanceof Cancellable) {
			Cancellable cancellable = (Cancellable) item;
			cancellable.cancel();
		}
	}

	static void tryClose(Object item) {
		if (item instanceof Cancellable) {
			Cancellable cancellable = (Cancellable) item;
			cancellable.close();
		}
	}

}
