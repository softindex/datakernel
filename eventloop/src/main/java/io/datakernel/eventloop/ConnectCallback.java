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

import io.datakernel.async.ExceptionCallback;

/**
 * This callback will be called for creating socket connection. It is implementation
 * of {@link ExceptionCallback} for handling exception during of a creating connection.
 */
public interface ConnectCallback extends ExceptionCallback {

	/**
	 * Method which creates socket connection with socketChannel and registers this connection to
	 * {@link Eventloop}.
	 *
	 * @param socketChannel socketChannel for creating new connection.
	 */
	AsyncTcpSocketImpl.EventHandler onConnect(AsyncTcpSocketImpl asyncTcpSocket);
}