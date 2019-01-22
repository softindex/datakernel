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

package io.datakernel.dataflow.graph;

import io.datakernel.dataflow.node.Node;
import io.datakernel.dataflow.server.DatagraphClient;

import java.net.InetSocketAddress;
import java.util.Collection;

/**
 * Defines a remote partition, which is represented by a datagraph client and a server address.
 */
public final class Partition {
	private final DatagraphClient client;
	private final InetSocketAddress address;

	/**
	 * Constructs a new remote partition with the given client and server address.
	 *
	 * @param client  datagraph client
	 * @param address server address
	 */
	public Partition(DatagraphClient client, InetSocketAddress address) {
		this.client = client;
		this.address = address;
	}

	public void execute(Collection<Node> nodes) {
		client.execute(address, nodes);
	}

	public InetSocketAddress getAddress() {
		return address;
	}

	@Override
	public String toString() {
		return "RemotePartition{address=" + address + '}';
	}
}
