package io.datakernel.rpc.client.sender;

import io.datakernel.rpc.client.RpcClientConnectionPool;
import io.datakernel.rpc.hash.HashFunction;
import io.datakernel.rpc.protocol.RpcMessage;
import org.junit.Test;

import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;

import static io.datakernel.rpc.client.sender.RequestSenderFactory.*;

import static java.util.Arrays.asList;
import static org.junit.Assert.assertEquals;

public class RequestSenderFactoryTest {

	@Test
	public void testFlatten() {
		List<List<String>> listOfList = new ArrayList<>();

		List<String> list1 = new ArrayList<>(asList("data1-1", "data1-2"));
		List<String> list2 = new ArrayList<>(asList("data2-1"));
		List<String> list3 = new ArrayList<>(asList("data3-1", "data3-2"));

		listOfList.add(list1);
		listOfList.add(list2);
		listOfList.add(list3);

		List<String> flatList = flatten(listOfList);

		List<String> expected = asList("data1-1", "data1-2", "data2-1", "data3-1", "data3-2");

		assertEquals(expected, flatList);
	}

	@Test
	public void testSenderStrategies() {

		InetSocketAddress address1 = new InetSocketAddress("localhost", 10001);
		InetSocketAddress address2 = new InetSocketAddress("localhost", 10002);
		InetSocketAddress address3 = new InetSocketAddress("localhost", 10003);
		InetSocketAddress address4 = new InetSocketAddress("localhost", 10004);
		InetSocketAddress address5 = new InetSocketAddress("localhost", 10005);

		HashFunction<RpcMessage.RpcMessageData> hashFunction = new HashFunction<RpcMessage.RpcMessageData>() {
			@Override
			public int hashCode(RpcMessage.RpcMessageData item) {
				return 0;
			}
		};

		RequestSenderFactory requestSenderFactory =
				firstAvailable(
						server(address1),
						roundRobin(
								servers(address2, address3),
								rendezvousHashing(hashFunction)
										.put(1, server(address4))
										.put(2, server(address5))
						));

		RequestSender strategy = requestSenderFactory.create(new RpcClientConnectionPool(new ArrayList<InetSocketAddress>()));

		System.out.println("success");
	}
}
