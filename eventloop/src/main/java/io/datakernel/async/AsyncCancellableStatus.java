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

/**
 * An instance of this class allows handle canceling
 */
public interface AsyncCancellableStatus {
	/**
	 * Checks if the action was cancelled
	 *
	 * @return true if was, false else
	 */
	boolean isCancelled();

	/**
	 * If action was cancelled calls onCancel of CancelNotifier
	 *
	 * @param cancelNotifier cancelNotifier
	 */
	void notifyOnCancel(CancelNotifier cancelNotifier);

	/**
	 * Interface which represents the handler for processing cancelling
	 */
	interface CancelNotifier {
		/**
		 * This method will be call when action is cancelling
		 */
		void onCancel();
	}
}
