package io.datakernel.rpc.client.sender;

import io.datakernel.rpc.client.RpcClientConnectionPool;
import io.datakernel.rpc.client.sender.helper.ResultCallbackStub;
import io.datakernel.rpc.client.sender.helper.RpcClientConnectionStub;
import io.datakernel.rpc.client.sender.helper.RpcMessageDataStub;
import io.datakernel.rpc.protocol.RpcMessage;
import org.junit.Test;

import java.net.InetSocketAddress;

import static java.util.Arrays.asList;
import static org.junit.Assert.assertEquals;

public class StrategyRoundRobinTest {

	private static final String HOST = "localhost";
	private static final int PORT_1 = 10001;
	private static final int PORT_2 = 10002;
	private static final int PORT_3 = 10003;
	private static final int PORT_4 = 10004;
	private static final int PORT_5 = 10005;
	private static final InetSocketAddress ADDRESS_1 = new InetSocketAddress(HOST, PORT_1);
	private static final InetSocketAddress ADDRESS_2 = new InetSocketAddress(HOST, PORT_2);
	private static final InetSocketAddress ADDRESS_3 = new InetSocketAddress(HOST, PORT_3);
	private static final InetSocketAddress ADDRESS_4 = new InetSocketAddress(HOST, PORT_4);
	private static final InetSocketAddress ADDRESS_5 = new InetSocketAddress(HOST, PORT_5);

	@Test
	public void itShouldSendRequestUsingRoundRobinAlgorithm() {
		RpcClientConnectionPool pool = new RpcClientConnectionPool(asList(ADDRESS_1, ADDRESS_2, ADDRESS_3));
		RpcClientConnectionStub connection1 = new RpcClientConnectionStub();
		RpcClientConnectionStub connection2 = new RpcClientConnectionStub();
		RpcClientConnectionStub connection3 = new RpcClientConnectionStub();
		RequestSendingStrategy singleServerStrategy1 = new StrategySingleServer(ADDRESS_1);
		RequestSendingStrategy singleServerStrategy2 = new StrategySingleServer(ADDRESS_2);
		RequestSendingStrategy singleServerStrategy3 = new StrategySingleServer(ADDRESS_3);
		RequestSendingStrategy roundRobinStrategy =
				new StrategyRoundRobin(asList(singleServerStrategy1, singleServerStrategy2, singleServerStrategy3));
		RequestSender senderRoundRobin;
		int timeout = 50;
		RpcMessage.RpcMessageData data = new RpcMessageDataStub();
		ResultCallbackStub callback = new ResultCallbackStub();
		int callsAmount = 5;

		pool.add(ADDRESS_1, connection1);
		pool.add(ADDRESS_2, connection2);
		pool.add(ADDRESS_3, connection3);
		senderRoundRobin = roundRobinStrategy.create(pool).get();
		for (int i = 0; i < callsAmount; i++) {
			senderRoundRobin.sendRequest(data, timeout, callback);
		}

		assertEquals(2, connection1.getCallsAmount());
		assertEquals(2, connection2.getCallsAmount());
		assertEquals(1, connection3.getCallsAmount());
	}

	@Test
	public void itShouldNotSendRequestToNonActiveSubSenders() {
		RpcClientConnectionPool pool
				= new RpcClientConnectionPool(asList(ADDRESS_1, ADDRESS_2, ADDRESS_3, ADDRESS_4, ADDRESS_5));
		RpcClientConnectionStub connection1 = new RpcClientConnectionStub();
		RpcClientConnectionStub connection2 = new RpcClientConnectionStub();
		RpcClientConnectionStub connection4 = new RpcClientConnectionStub();
		RequestSendingStrategy singleServerStrategy1 = new StrategySingleServer(ADDRESS_1);
		RequestSendingStrategy singleServerStrategy2 = new StrategySingleServer(ADDRESS_2);
		RequestSendingStrategy singleServerStrategy3 = new StrategySingleServer(ADDRESS_3);
		RequestSendingStrategy singleServerStrategy4 = new StrategySingleServer(ADDRESS_4);
		RequestSendingStrategy singleServerStrategy5 = new StrategySingleServer(ADDRESS_5);
		RequestSendingStrategy roundRobinStrategy =
				new StrategyRoundRobin(asList(singleServerStrategy1, singleServerStrategy2, singleServerStrategy3,
						singleServerStrategy4, singleServerStrategy5));
		RequestSender senderRoundRobin;
		int timeout = 50;
		RpcMessage.RpcMessageData data = new RpcMessageDataStub();
		ResultCallbackStub callback = new ResultCallbackStub();
		int callsAmount = 10;

		pool.add(ADDRESS_1, connection1);
		pool.add(ADDRESS_2, connection2);
		pool.add(ADDRESS_4, connection4);
		// we don't add connections for ADDRESS_3 and ADDRESS_5
		senderRoundRobin = roundRobinStrategy.create(pool).get();
		for (int i = 0; i < callsAmount; i++) {
			senderRoundRobin.sendRequest(data, timeout, callback);
		}

		assertEquals(4, connection1.getCallsAmount());
		assertEquals(3, connection2.getCallsAmount());
		assertEquals(3, connection4.getCallsAmount());
	}

	@Test(expected = Exception.class)
	public void itShouldThrowExceptionWhenSubSendersListIsNull() {
		RequestSendingStrategy strategy = new StrategyRoundRobin(null);
	}
}
