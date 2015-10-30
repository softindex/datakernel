package io.datakernel.rpc.client.sender;

import io.datakernel.rpc.client.RpcClientConnectionPool;
import io.datakernel.rpc.client.sender.helper.ResultCallbackStub;
import io.datakernel.rpc.client.sender.helper.RpcClientConnectionStub;
import io.datakernel.rpc.client.sender.helper.RpcMessageDataStub;
import io.datakernel.rpc.protocol.RpcMessage;
import org.junit.Test;

import java.net.InetSocketAddress;
import java.util.ArrayList;

import static java.util.Arrays.asList;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class RequestSenderToFirstTest {

	private static final String HOST = "localhost";
	private static final int PORT_1 = 10001;
	private static final int PORT_2 = 10002;
	private static final int PORT_3 = 10003;
	private static final InetSocketAddress ADDRESS_1 = new InetSocketAddress(HOST, PORT_1);
	private static final InetSocketAddress ADDRESS_2 = new InetSocketAddress(HOST, PORT_2);
	private static final InetSocketAddress ADDRESS_3 = new InetSocketAddress(HOST, PORT_3);

	@Test
	public void itShouldBeActiveWhenThereIsAtLeastOneActiveSubSender() {
		RpcClientConnectionPool pool = new RpcClientConnectionPool(asList(ADDRESS_1, ADDRESS_2, ADDRESS_3));
		RpcClientConnectionStub connection = new RpcClientConnectionStub();
		pool.add(ADDRESS_2, connection);
		RequestSender senderToServer1 = new RequestSenderToSingleServer(ADDRESS_1, pool);
		RequestSender senderToServer2 = new RequestSenderToSingleServer(ADDRESS_2, pool);
		RequestSenderToFirst senderToFirst = new RequestSenderToFirst(asList(senderToServer1, senderToServer2));

		assertFalse(senderToServer1.isActive());
		assertTrue(senderToServer2.isActive());
		assertTrue(senderToFirst.isActive());
	}

	@Test
	public void itShouldNotBeActiveWhenThereIsNoActiveSubSenders() {
		RpcClientConnectionPool pool = new RpcClientConnectionPool(asList(ADDRESS_1, ADDRESS_2, ADDRESS_3));
		// no connections were added to pool
		RequestSender senderToServer1 = new RequestSenderToSingleServer(ADDRESS_1, pool);
		RequestSender senderToServer2 = new RequestSenderToSingleServer(ADDRESS_2, pool);
		RequestSender senderToServer3 = new RequestSenderToSingleServer(ADDRESS_3, pool);
		RequestSenderToFirst senderToFirst =
				new RequestSenderToFirst(asList(senderToServer1, senderToServer2, senderToServer3));


		assertFalse(senderToServer1.isActive());
		assertFalse(senderToServer2.isActive());
		assertFalse(senderToServer3.isActive());
		assertFalse(senderToFirst.isActive());
	}

	@Test
	public void itShouldNotBeActiveWhenThereIsNoSubSenders() {
		RequestSenderToFirst sender = new RequestSenderToFirst(new ArrayList<RequestSender>());

		assertFalse(sender.isActive());
	}

	@Test
	public void itShouldSendRequestToFirstAvailableSubSender() {
		RpcClientConnectionPool pool = new RpcClientConnectionPool(asList(ADDRESS_1, ADDRESS_2, ADDRESS_3));

		RpcClientConnectionStub connection1 = new RpcClientConnectionStub();
		RpcClientConnectionStub connection2 = new RpcClientConnectionStub();
		RpcClientConnectionStub connection3 = new RpcClientConnectionStub();

		RequestSender senderToServer1;
		RequestSender senderToServer2;
		RequestSender senderToServer3;
		RequestSenderToFirst senderToFirstAvailable;

		int timeout = 50;
		RpcMessage.RpcMessageData data = new RpcMessageDataStub();
		ResultCallbackStub callback = new ResultCallbackStub();

		int callsToSender1 = 10;
		int callsToSender2 = 25;
		int callsToSender3 = 32;




		pool.add(ADDRESS_1, connection1);
		pool.add(ADDRESS_2, connection2);
		pool.add(ADDRESS_3, connection3);
		senderToServer1 = new RequestSenderToSingleServer(ADDRESS_1, pool);
		senderToServer2 = new RequestSenderToSingleServer(ADDRESS_2, pool);
		senderToServer3 = new RequestSenderToSingleServer(ADDRESS_3, pool);
		senderToFirstAvailable = new RequestSenderToFirst(asList(senderToServer1, senderToServer2, senderToServer3));
		for (int i = 0; i < callsToSender1; i++) {
			senderToFirstAvailable.sendRequest(data, timeout, callback);
		}

		pool.remove(ADDRESS_1);
		// we should recreate sender after changing in pool
		senderToServer1 = new RequestSenderToSingleServer(ADDRESS_1, pool);
		senderToServer2 = new RequestSenderToSingleServer(ADDRESS_2, pool);
		senderToServer3 = new RequestSenderToSingleServer(ADDRESS_3, pool);
		senderToFirstAvailable = new RequestSenderToFirst(asList(senderToServer1, senderToServer2, senderToServer3));
		for (int i = 0; i < callsToSender2; i++) {
			senderToFirstAvailable.sendRequest(data, timeout, callback);
		}

		pool.remove(ADDRESS_2);
		// we should recreate sender after changing in pool
		senderToServer1 = new RequestSenderToSingleServer(ADDRESS_1, pool);
		senderToServer2 = new RequestSenderToSingleServer(ADDRESS_2, pool);
		senderToServer3 = new RequestSenderToSingleServer(ADDRESS_3, pool);
		senderToFirstAvailable = new RequestSenderToFirst(asList(senderToServer1, senderToServer2, senderToServer3));
		for (int i = 0; i < callsToSender3; i++) {
			senderToFirstAvailable.sendRequest(data, timeout, callback);
		}

		

		assertEquals(callsToSender1, connection1.getCallsAmount());
		assertEquals(callsToSender2, connection2.getCallsAmount());
		assertEquals(callsToSender3, connection3.getCallsAmount());
	}

	@Test(expected = Exception.class)
	public void itShouldThrowExceptionWhenSubSendersListIsNull() {
		RequestSenderToFirst sender = new RequestSenderToFirst(null);
	}
}
