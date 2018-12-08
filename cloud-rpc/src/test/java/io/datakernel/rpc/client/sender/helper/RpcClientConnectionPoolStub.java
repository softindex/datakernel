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

package io.datakernel.rpc.client.sender.helper;

import io.datakernel.rpc.client.RpcClientConnectionPool;
import io.datakernel.rpc.client.sender.RpcSender;

import java.net.InetSocketAddress;
import java.util.HashMap;
import java.util.Map;

public class RpcClientConnectionPoolStub implements RpcClientConnectionPool {
	private final Map<InetSocketAddress, RpcSender> connections = new HashMap<>();

	public void put(InetSocketAddress address, RpcSender connection) {
		connections.put(address, connection);
	}

	public void remove(InetSocketAddress address) {
		connections.remove(address);
	}

	@Override
	public RpcSender get(InetSocketAddress address) {
		return connections.get(address);
	}
}
