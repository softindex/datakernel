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

import com.google.common.base.Optional;
import io.datakernel.async.ResultCallback;
import io.datakernel.rpc.client.RpcClientConnection;
import io.datakernel.rpc.client.RpcClientConnectionPool;
import io.datakernel.rpc.protocol.RpcMessage;

import java.net.InetSocketAddress;
import java.util.List;

import static com.google.common.base.Preconditions.checkNotNull;
import static java.util.Arrays.asList;

public final class StrategySingleServer extends AbstractRequestSendingStrategy implements SingleSenderStrategy {

	private final InetSocketAddress address;

	public StrategySingleServer(InetSocketAddress address) {
		checkNotNull(address);
		this.address = address;
	}

	@Override
	protected List<Optional<RequestSender>> createAsList(RpcClientConnectionPool pool) {
		return asList(create(pool));
	}

	@Override
	public Optional<RequestSender> create(RpcClientConnectionPool pool) {
		RpcClientConnection connection = pool.get(address);
		if (connection != null) {
			return Optional.<RequestSender>of(new RequestSenderToSingleServer(connection));
		} else {
			return Optional.absent();
		}
	}

	final static class RequestSenderToSingleServer implements RequestSender {
		private final RpcClientConnection connection;

		public RequestSenderToSingleServer(RpcClientConnection connection) {
			this.connection = checkNotNull(connection);
		}

		@Override
		public <T extends RpcMessage.RpcMessageData> void sendRequest(RpcMessage.RpcMessageData request, int timeout,
		                                                              ResultCallback<T> callback) {

			connection.callMethod(request, timeout, callback);
		}
	}
}
