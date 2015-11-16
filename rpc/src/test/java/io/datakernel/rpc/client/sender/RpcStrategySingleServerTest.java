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

import io.datakernel.rpc.client.RpcClientConnectionPool;
import io.datakernel.rpc.client.sender.helper.ResultCallbackStub;
import io.datakernel.rpc.client.sender.helper.RpcClientConnectionStub;
import io.datakernel.rpc.client.sender.helper.RpcMessageDataStub;
import org.junit.Test;

import java.net.InetSocketAddress;

import static java.util.Arrays.asList;
import static org.junit.Assert.*;

public class RpcStrategySingleServerTest {

	private static final String HOST = "localhost";
	private static final int PORT = 10000;
	private static final InetSocketAddress ADDRESS = new InetSocketAddress(HOST, PORT);

	@Test
	public void itShouldBeCreatedWhenThereIsConnectionInPool() {
		RpcClientConnectionPool pool = new RpcClientConnectionPool(asList(ADDRESS));
		RpcClientConnectionStub connection = new RpcClientConnectionStub();
		pool.add(ADDRESS, connection);
		RpcStrategySingleServer strategySingleServer = new RpcStrategySingleServer(ADDRESS);

		RpcRequestSenderHolder singleServer = strategySingleServer.create(pool);

		assertTrue(singleServer.isSenderPresent());
	}

	@Test
	public void itShouldNotBeCreatedWhenThereIsNoConnectionInPool() {
		RpcClientConnectionPool pool = new RpcClientConnectionPool(asList(ADDRESS));
		// no connections were added to pool
		RpcStrategySingleServer strategySingleServer = new RpcStrategySingleServer(ADDRESS);

		RpcRequestSenderHolder singleServer = strategySingleServer.create(pool);

		assertFalse(singleServer.isSenderPresent());
	}

	@Test(expected = Exception.class)
	public void itShouldThrowExceptionWhenAddressIsNull() {
		RpcClientConnectionPool pool = new RpcClientConnectionPool(asList(ADDRESS));
		RpcStrategySingleServer strategySingleServer = new RpcStrategySingleServer(null);
	}

	@Test(expected = Exception.class)
	public void itShouldThrowExceptionWhenConnectionPoolIsNull() {
		RpcStrategySingleServer strategySingleServer = new RpcStrategySingleServer(null);
		strategySingleServer.create(null);
	}

	@Test
	public void itShouldProcessAllCalls() {
		RpcClientConnectionPool pool = new RpcClientConnectionPool(asList(ADDRESS));
		RpcClientConnectionStub connection = new RpcClientConnectionStub();
		pool.add(ADDRESS, connection);
		RpcStrategySingleServer strategySingleServer = new RpcStrategySingleServer(ADDRESS);
		RpcRequestSender sender = strategySingleServer.create(pool).getSender();
		final int calls = 100;
		int timeout = 50;
		RpcMessageDataStub data = new RpcMessageDataStub();
		ResultCallbackStub callback = new ResultCallbackStub();

		for (int i = 0; i < calls; i++) {
			sender.sendRequest(data, timeout, callback);
		}

		assertEquals(calls, connection.getCallsAmount());
	}
}
