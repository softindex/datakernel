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

import com.google.common.base.Throwables;

/**
 * Represents the runnable which will be ran with exception, which will be threw from runWithException()
 */
public abstract class RunnableWithException implements Runnable {

	/**
	 * Throws the exception
	 */
	public abstract void runWithException() throws Exception;

	@Override
	public final void run() {
		try {
			runWithException();
		} catch (Exception e) {
			throw Throwables.propagate(e);
		}
	}

}
