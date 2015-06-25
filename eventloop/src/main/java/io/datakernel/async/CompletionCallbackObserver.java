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

import static com.google.common.base.Preconditions.checkState;
import static com.google.common.base.Throwables.propagate;

/**
 * Observer, used to check completion of some asynchronous action.
 * Saves completion flag or exception upon the {@code onComplete} or {@code onException} call.
 */
public class CompletionCallbackObserver implements CompletionCallback {
	private boolean complete;
	private Exception exception;

	@Override
	public void onComplete() {
		this.complete = true;
	}

	@Override
	public void onException(Exception exception) {
		this.exception = exception;
	}

	public boolean isComplete() {
		return complete;
	}

	public Exception getException() {
		return exception;
	}

	/**
	 * If exception occurred during the observed action, it is propagated to the caller of this method.
	 * If it does not, the completion flag is checked. If the action is not completed, {@code IllegalStateException} is thrown.
	 */
	public void check() {
		if (exception != null) {
			throw propagate(exception);
		}
		checkState(complete, "Not complete");
	}
}
