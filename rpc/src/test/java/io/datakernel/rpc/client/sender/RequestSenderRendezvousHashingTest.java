package io.datakernel.rpc.client.sender;

import io.datakernel.async.ResultCallback;
import io.datakernel.rpc.client.RpcClientConnectionPool;
import io.datakernel.rpc.client.sender.helper.ResultCallbackStub;
import io.datakernel.rpc.client.sender.helper.RpcClientConnectionStub;
import io.datakernel.rpc.hash.HashFunction;
import io.datakernel.rpc.protocol.RpcMessage;
import org.junit.Test;

import java.net.InetSocketAddress;
import java.util.HashMap;
import java.util.Map;

import static io.datakernel.rpc.client.sender.RequestSenderRendezvousHashing.DefaultBucketHashFunction;
import static io.datakernel.rpc.client.sender.RequestSenderRendezvousHashing.RendezvousHashBucket;
import static java.util.Arrays.asList;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

public class RequestSenderRendezvousHashingTest {

	final boolean ACTIVE = true;
	final boolean NON_ACTIVE = false;

	private static final String HOST = "localhost";
	private static final int PORT_1 = 10001;
	private static final int PORT_2 = 10002;
	private static final int PORT_3 = 10003;
	private static final InetSocketAddress ADDRESS_1 = new InetSocketAddress(HOST, PORT_1);
	private static final InetSocketAddress ADDRESS_2 = new InetSocketAddress(HOST, PORT_2);
	private static final InetSocketAddress ADDRESS_3 = new InetSocketAddress(HOST, PORT_3);

	@Test
	public void itShouldDistributeCallsBetweenActiveSenders() {
		RpcClientConnectionPool pool = new RpcClientConnectionPool(asList(ADDRESS_1, ADDRESS_2, ADDRESS_3));

		RpcClientConnectionStub connection1 = new RpcClientConnectionStub();
		RpcClientConnectionStub connection2 = new RpcClientConnectionStub();
		RpcClientConnectionStub connection3 = new RpcClientConnectionStub();

		Map<Object, RequestSender> keyToSender;

		int key1 = 1;
		int key2 = 2;
		int key3 = 3;

		RequestSender senderToServer1;
		RequestSender senderToServer2;
		RequestSender senderToServer3;
		RequestSenderRendezvousHashing rendezvousSender;

		HashFunction<RpcMessage.RpcMessageData> hashFunction = new RequestDataHashFunction();

		int callsPerLoop = 10000;

		int timeout = 50;
		ResultCallbackStub callback = new ResultCallbackStub();



		pool.add(ADDRESS_1, connection1);
		pool.add(ADDRESS_2, connection2);
		pool.add(ADDRESS_3, connection3);
		senderToServer1 = new RequestSenderToSingleServer(ADDRESS_1, pool);
		senderToServer2 = new RequestSenderToSingleServer(ADDRESS_2, pool);
		senderToServer3 = new RequestSenderToSingleServer(ADDRESS_3, pool);
		keyToSender = new HashMap<>();
		keyToSender.put(key1, senderToServer1);
		keyToSender.put(key2, senderToServer2);
		keyToSender.put(key3, senderToServer3);
		rendezvousSender = new RequestSenderRendezvousHashing(keyToSender, hashFunction);

		for (int i = 0; i < callsPerLoop; i++) {
			rendezvousSender.sendRequest(new RequestData(i), timeout, callback);
		}



		pool.remove(ADDRESS_1);
		senderToServer1 = new RequestSenderToSingleServer(ADDRESS_1, pool);
		senderToServer2 = new RequestSenderToSingleServer(ADDRESS_2, pool);
		senderToServer3 = new RequestSenderToSingleServer(ADDRESS_3, pool);
		keyToSender = new HashMap<>();
		keyToSender.put(key1, senderToServer1);
		keyToSender.put(key2, senderToServer2);
		keyToSender.put(key3, senderToServer3);
		rendezvousSender = new RequestSenderRendezvousHashing(keyToSender, hashFunction);
		for (int i = 0; i < callsPerLoop; i++) {
			rendezvousSender.sendRequest(new RequestData(i), timeout, callback);
		}



		pool.remove(ADDRESS_3);
		senderToServer1 = new RequestSenderToSingleServer(ADDRESS_1, pool);
		senderToServer2 = new RequestSenderToSingleServer(ADDRESS_2, pool);
		senderToServer3 = new RequestSenderToSingleServer(ADDRESS_3, pool);
		keyToSender = new HashMap<>();
		keyToSender.put(key1, senderToServer1);
		keyToSender.put(key2, senderToServer2);
		keyToSender.put(key3, senderToServer3);
		rendezvousSender = new RequestSenderRendezvousHashing(keyToSender, hashFunction);
		for (int i = 0; i < callsPerLoop; i++) {
			rendezvousSender.sendRequest(new RequestData(i), timeout, callback);
		}




		int expectedCallsOfConnection1 = callsPerLoop / 3;
		int expectedCallsOfConnection2 = (callsPerLoop / 3) + (callsPerLoop / 2) + callsPerLoop;
		int expectedCallsOfConnection3 = (callsPerLoop / 3) + (callsPerLoop / 2);

		double delta = callsPerLoop / 20.0;

		assertEquals(expectedCallsOfConnection1, connection1.getCallsAmount(), delta);
		assertEquals(expectedCallsOfConnection2, connection2.getCallsAmount(), delta);
		assertEquals(expectedCallsOfConnection3, connection3.getCallsAmount(), delta);

	}

	@Test
	public void testRendezvousHashBucket() {
		final int SENDERS_AMOUNT = 4;
		final int DEFAULT_BUCKET_CAPACITY = 1 << 11;
		final Map<Object, RequestSender> keyToSender = new HashMap<>(SENDERS_AMOUNT);
		for (int i = 0; i < SENDERS_AMOUNT; i++) {
			keyToSender.put(i, new RequestSenderStub(i, ACTIVE));
		}
		RendezvousHashBucket hashBucket;


		hashBucket = new RendezvousHashBucket(keyToSender, new DefaultBucketHashFunction(), DEFAULT_BUCKET_CAPACITY);
		RequestSender[] baseBucket = new RequestSender[DEFAULT_BUCKET_CAPACITY];
		for (int i = 0; i < DEFAULT_BUCKET_CAPACITY; i++) {
			baseBucket[i] = hashBucket.chooseSender(i);
		}


		int key1 = 1;
		RequestSender sender1 = keyToSender.get(key1);
		keyToSender.put(key1, new RequestSenderStub(key1, NON_ACTIVE));
		hashBucket = new RendezvousHashBucket(keyToSender, new DefaultBucketHashFunction(), DEFAULT_BUCKET_CAPACITY);
		for (int i = 0; i < DEFAULT_BUCKET_CAPACITY; i++) {
			if (!baseBucket[i].equals(sender1))
				assertEquals(baseBucket[i], hashBucket.chooseSender(i));
			else
				assertFalse(hashBucket.chooseSender(i).equals(sender1));
		}


		int key2 = 2;
		RequestSender sender2 = keyToSender.get(key2);
		keyToSender.put(key2, new RequestSenderStub(key2, NON_ACTIVE));
		hashBucket = new RendezvousHashBucket(keyToSender, new DefaultBucketHashFunction(), DEFAULT_BUCKET_CAPACITY);
		for (int i = 0; i < DEFAULT_BUCKET_CAPACITY; i++) {
			if (!baseBucket[i].equals(sender1) && !baseBucket[i].equals(sender2))
				assertEquals(baseBucket[i], hashBucket.chooseSender(i));
			else
				assertFalse(hashBucket.chooseSender(i).equals(sender1));
			assertFalse(hashBucket.chooseSender(i).equals(sender2));
		}


		keyToSender.put(key1, new RequestSenderStub(key1, ACTIVE));
		hashBucket = new RendezvousHashBucket(keyToSender, new DefaultBucketHashFunction(), DEFAULT_BUCKET_CAPACITY);
		for (int i = 0; i < DEFAULT_BUCKET_CAPACITY; i++) {
			if (!baseBucket[i].equals(sender2))
				assertEquals(baseBucket[i], hashBucket.chooseSender(i));
			else
				assertFalse(hashBucket.chooseSender(i).equals(sender2));
		}


		keyToSender.put(key2, new RequestSenderStub(key2, ACTIVE));
		hashBucket = new RendezvousHashBucket(keyToSender, new DefaultBucketHashFunction(), DEFAULT_BUCKET_CAPACITY);
		for (int i = 0; i < DEFAULT_BUCKET_CAPACITY; i++) {
			assertEquals(baseBucket[i], hashBucket.chooseSender(i));
		}
	}

	static class RequestSenderStub implements RequestSender {

		private final int id;
		private final boolean active;
		private int sendRequestCalls;

		public RequestSenderStub(int id, boolean active) {
			this.id = id;
			this.active = active;
			this.sendRequestCalls = 0;
		}

		@Override
		public <T extends RpcMessage.RpcMessageData> void sendRequest(RpcMessage.RpcMessageData request, int timeout,
		                                                              ResultCallback<T> callback) {
			++sendRequestCalls;
		}

		@Override
		public boolean isActive() {
			return active;
		}

		public int getId() {
			return id;
		}

		public int getSendRequestCalls() {
			return sendRequestCalls;
		}

		@Override
		public boolean equals(Object obj) {
			return ((RequestSenderStub)obj).getId() == id;
		}

		@Override
		public int hashCode() {
			return id;
		}
	}

	public static class RequestData implements RpcMessage.RpcMessageData {

		private final int key;

		public RequestData(int key) {
			this.key = key;
		}

		public int getKey() {
			return key;
		}

		@Override
		public boolean isMandatory() {
			return false;
		}
	}

	public class RequestDataHashFunction implements HashFunction<RpcMessage.RpcMessageData> {

		@Override
		public int hashCode(RpcMessage.RpcMessageData item) {
			return ((RequestData)item).getKey();
		}
	}
}
