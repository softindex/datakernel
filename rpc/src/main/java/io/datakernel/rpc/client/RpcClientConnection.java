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

package io.datakernel.rpc.client;

import io.datakernel.async.ResultCallback;
import io.datakernel.eventloop.SocketConnection;
import io.datakernel.rpc.protocol.RpcConnection;
import io.datakernel.rpc.protocol.RpcMessage;

public interface RpcClientConnection extends RpcConnection, RpcClientConnectionMBean {

	<I, O> void callMethod(
			I request, int timeout, ResultCallback<O> callback);

	void close();

	SocketConnection getSocketConnection();

	interface StatusListener {
		void onOpen(RpcClientConnection connection);

		void onClosed();
	}
}
