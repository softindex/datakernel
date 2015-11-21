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

import io.datakernel.async.ResultCallback;
import io.datakernel.rpc.client.RpcClientConnection;
import io.datakernel.rpc.client.RpcClientConnectionPool;

import java.net.InetSocketAddress;
import java.util.Collections;
import java.util.Set;

import static io.datakernel.util.Preconditions.checkNotNull;

public final class RpcStrategySingleServer implements RpcRequestSendingStrategy {

	private final InetSocketAddress address;

	public RpcStrategySingleServer(InetSocketAddress address) {
		checkNotNull(address);
		this.address = address;
	}

	@Override
	public Set<InetSocketAddress> getAddresses() {
		return Collections.singleton(address);
	}

	@Override
	public RpcRequestSender createSender(RpcClientConnectionPool pool) {
		RpcClientConnection connection = pool.get(address);
		if (connection == null)
			return null;
		return new RequestSenderToSingleServer(connection);
	}

	final static class RequestSenderToSingleServer implements RpcRequestSender {
		private final RpcClientConnection connection;

		public RequestSenderToSingleServer(RpcClientConnection connection) {
			this.connection = checkNotNull(connection);
		}

		@Override
		public <T> void sendRequest(Object request, int timeout, ResultCallback<T> callback) {
			connection.callMethod(request, timeout, callback);
		}
	}
}
