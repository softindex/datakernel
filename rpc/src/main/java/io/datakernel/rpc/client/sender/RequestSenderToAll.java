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

package io.datakernel.rpc.client.sender;

import io.datakernel.annotation.NioThread;
import io.datakernel.async.FirstResultCallback;
import io.datakernel.async.ResultCallback;
import io.datakernel.rpc.client.RpcClientConnection;
import io.datakernel.rpc.client.RpcClientConnectionPool;
import io.datakernel.rpc.protocol.RpcMessage;

import javax.management.openmbean.CompositeData;
import javax.management.openmbean.OpenDataException;
import java.net.InetSocketAddress;

import static com.google.common.base.Preconditions.checkNotNull;

@NioThread
final class RequestSenderToAll implements RequestSender {
	private static final RequestSenderToNone NO_AVAILABLE_CONNECTION = new RequestSenderToNone();
	private final RpcClientConnectionPool connections;

	public RequestSenderToAll(RpcClientConnectionPool connections) {
		this.connections = checkNotNull(connections);
	}

	@Override
	public <T extends RpcMessage.RpcMessageData> void sendRequest(RpcMessage.RpcMessageData request, int timeout, final ResultCallback<T> callback) {
		checkNotNull(callback);
		int calls = 0;
		FirstResultCallback<T> resultCallback = new FirstResultCallback<>(callback);
		for (InetSocketAddress address : connections.addresses()) {
			RpcClientConnection connection = connections.get(address);
			if (connection == null) {
				continue;
			}
			calls++;
			connection.callMethod(request, timeout, resultCallback);
		}
		if (calls == 0) {
			callback.onException(NO_AVAILABLE_CONNECTION);
		} else {
			resultCallback.resultOf(calls);
		}
	}

	@Override
	public void onConnectionsUpdated() {
	}

	// JMX
	@Override
	public void resetStats() {
	}

	@Override
	public CompositeData getRequestSenderInfo() throws OpenDataException {
		return null;
	}
}
