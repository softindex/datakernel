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

import io.datakernel.eventloop.Eventloop;

import static io.datakernel.eventloop.Eventloop.getCurrentEventloop;

/**
 * This callback handles exceptions.
 */
public abstract class ExceptionCallback {
	public ExceptionCallback() {
		CallbackRegistry.register(this);
	}

	/**
	 * Handles exception
	 *
	 * @param e exception that was throwing
	 */
	public final void setException(Exception e) {
		assert e != null;
		CallbackRegistry.complete(this);
		onException(e);
	}

	public final void postException(final Exception e) {
		postException(getCurrentEventloop(), e);
	}

	public final void postException(Eventloop eventloop, final Exception e) {
		eventloop.post(new Runnable() {
			@Override
			public void run() {
				setException(e);
			}
		});
	}

	protected abstract void onException(Exception e);
}