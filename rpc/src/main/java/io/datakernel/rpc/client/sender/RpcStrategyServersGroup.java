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

import io.datakernel.rpc.client.RpcClientConnection;
import io.datakernel.rpc.client.RpcClientConnectionPool;

import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.List;

import static io.datakernel.util.Preconditions.checkArgument;
import static io.datakernel.util.Preconditions.checkNotNull;

public final class RpcStrategyServersGroup implements RpcRequestSendingStrategy {

	private List<InetSocketAddress> addresses;

	RpcStrategyServersGroup(List<InetSocketAddress> addresses) {
		checkNotNull(addresses);
		checkArgument(addresses.size() > 0, "at least one address must be present");
		this.addresses = addresses;
	}

	@Override
	public List<RpcRequestSenderHolder> createAsList(RpcClientConnectionPool pool) {
		List<RpcRequestSenderHolder> senderHolders = new ArrayList<>();
		for (InetSocketAddress address : addresses) {
			RpcClientConnection connection = pool.get(address);
			if (connection != null) {
				senderHolders.add(
						RpcRequestSenderHolder.of(new RpcStrategySingleServer.RequestSenderToSingleServer(connection))
				);
			} else {
				senderHolders.add(RpcRequestSenderHolder.absent());
			}
		}
		return senderHolders;
	}

	@Override
	public RpcRequestSenderHolder create(RpcClientConnectionPool pool) {
		throw new UnsupportedOperationException();
	}
}
