package io.datakernel.rpc.client.sender;

import com.google.common.base.Optional;
import io.datakernel.rpc.client.RpcClientConnectionPool;
import io.datakernel.rpc.client.sender.helper.ResultCallbackStub;
import io.datakernel.rpc.client.sender.helper.RpcClientConnectionStub;
import io.datakernel.rpc.client.sender.helper.RpcMessageDataStub;
import io.datakernel.rpc.protocol.RpcMessage;
import org.junit.Test;

import java.net.InetSocketAddress;

import static java.util.Arrays.asList;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class RequestSenderToSingleServerTest {

	private static final String HOST = "localhost";
	private static final int PORT = 10000;
	private static final InetSocketAddress ADDRESS = new InetSocketAddress(HOST, PORT);

	@Test
	public void itShouldBeCreatedWhenThereIsConnectionInPool() {
		RpcClientConnectionPool pool = new RpcClientConnectionPool(asList(ADDRESS));
		RpcClientConnectionStub connection = new RpcClientConnectionStub();
		pool.add(ADDRESS, connection);
		StrategySingleServer strategySingleServer = new StrategySingleServer(ADDRESS);

		Optional<RequestSender> singleServer = strategySingleServer.create(pool);

		assertTrue(singleServer.isPresent());
	}

	@Test
	public void itShouldNotBeCreatedWhenThereIsNoConnectionInPool() {
		RpcClientConnectionPool pool = new RpcClientConnectionPool(asList(ADDRESS));
		// no connections were added to pool
		StrategySingleServer strategySingleServer = new StrategySingleServer(ADDRESS);

		Optional<RequestSender> singleServer = strategySingleServer.create(pool);

		assertFalse(singleServer.isPresent());
	}

	@Test(expected = Exception.class)
	public void itShouldThrowExceptionWhenAddressIsNull() {
		RpcClientConnectionPool pool = new RpcClientConnectionPool(asList(ADDRESS));
		StrategySingleServer strategySingleServer = new StrategySingleServer(null);
	}

	@Test(expected = Exception.class)
	public void itShouldThrowExceptionWhenConnectionPoolIsNull() {
		StrategySingleServer strategySingleServer = new StrategySingleServer(null);
		strategySingleServer.create(null);
	}

	@Test
	public void itShouldProcessAllCalls() {
		RpcClientConnectionPool pool = new RpcClientConnectionPool(asList(ADDRESS));
		RpcClientConnectionStub connection = new RpcClientConnectionStub();
		pool.add(ADDRESS, connection);
		StrategySingleServer strategySingleServer = new StrategySingleServer(ADDRESS);
		RequestSender sender = strategySingleServer.create(pool).get();
		final int calls = 100;
		int timeout = 50;
		RpcMessage.RpcMessageData data = new RpcMessageDataStub();
		ResultCallbackStub callback = new ResultCallbackStub();

		for (int i = 0; i < calls; i++) {
			sender.sendRequest(data, timeout, callback);
		}

		assertEquals(calls, connection.getCallsAmount());
	}
}
