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

import io.datakernel.rpc.client.sender.helper.ResultCallbackStub;
import io.datakernel.rpc.client.sender.helper.RpcClientConnectionPoolStub;
import io.datakernel.rpc.client.sender.helper.RpcMessageDataStub;
import io.datakernel.rpc.client.sender.helper.RpcSenderStub;
import org.junit.Test;

import java.net.InetSocketAddress;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class RpcStrategySingleServerTest {

	private static final String HOST = "localhost";
	private static final int PORT = 10000;
	private static final InetSocketAddress ADDRESS = new InetSocketAddress(HOST, PORT);

	@Test
	public void itShouldBeCreatedWhenThereIsConnectionInPool() {
		RpcClientConnectionPoolStub pool = new RpcClientConnectionPoolStub();
		RpcSenderStub connection = new RpcSenderStub();
		pool.put(ADDRESS, connection);
		RpcStrategySingleServer strategySingleServer = new RpcStrategySingleServer(ADDRESS);

		RpcSender sender = strategySingleServer.createSender(pool);

		assertTrue(sender != null);
	}

	@Test
	public void itShouldNotBeCreatedWhenThereIsNoConnectionInPool() {
		RpcClientConnectionPoolStub pool = new RpcClientConnectionPoolStub();
		// no connections were added to pool
		RpcStrategySingleServer strategySingleServer = new RpcStrategySingleServer(ADDRESS);

		RpcSender sender = strategySingleServer.createSender(pool);

		assertTrue(sender == null);
	}

	@Test
	public void itShouldProcessAllCalls() {
		RpcClientConnectionPoolStub pool = new RpcClientConnectionPoolStub();
		RpcSenderStub connection = new RpcSenderStub();
		pool.put(ADDRESS, connection);
		RpcStrategySingleServer strategySingleServer = new RpcStrategySingleServer(ADDRESS);
		RpcSender sender = strategySingleServer.createSender(pool);
		final int calls = 100;
		int timeout = 50;
		RpcMessageDataStub data = new RpcMessageDataStub();
		ResultCallbackStub callback = new ResultCallbackStub();

		for (int i = 0; i < calls; i++) {
			sender.sendRequest(data, timeout, callback);
		}

		assertEquals(calls, connection.getSendsNumber());
	}
}
