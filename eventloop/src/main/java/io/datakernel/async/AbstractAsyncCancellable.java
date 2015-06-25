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

import java.util.ArrayList;
import java.util.List;

/**
 * An action of object of this class  can be cancelled.
 */
public abstract class AbstractAsyncCancellable implements AsyncCancellable, AsyncCancellableStatus {
	private boolean cancelled;
	private List<CancelNotifier> cancelNotifiers;

	/**
	 * Checks if the action was cancelled
	 *
	 * @return true if was, false else
	 */
	@Override
	public final boolean isCancelled() {
		return cancelled;
	}

	/**
	 * If the action was cancelled calls onCancel of CancelNotifier
	 */
	@Override
	public final void notifyOnCancel(CancelNotifier cancelNotifier) {
		if (!cancelled) {
			if (cancelNotifiers == null)
				cancelNotifiers = new ArrayList<>();
			cancelNotifiers.add(cancelNotifier);
		} else {
			cancelNotifier.onCancel();
		}
	}

	/**
	 * This method should cancel some action that it has began
	 */
	@Override
	public final void cancel() {
		if (!cancelled) {
			cancelled = true;
			if (cancelNotifiers != null) {
				for (CancelNotifier notifier : cancelNotifiers) {
					notifier.onCancel();
				}
				cancelNotifiers = null;
			}
		}
	}
}
