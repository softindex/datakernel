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

import io.datakernel.async.CompletionCallback;

public interface NioService {
	NioEventloop getNioEventloop();

	/**
	 * Starts this component asynchronously.
	 * Callback completes immediately if the component is already running.
	 *
	 * @param callback callback which will be called after completion starting
	 */
	void start(CompletionCallback callback);

	/**
	 * Stops this component asynchronously.
	 * Callback completes immediately if the component is not running / already stopped.
	 *
	 * @param callback callback which will be called after completion stopping
	 */
	void stop(CompletionCallback callback);

}
