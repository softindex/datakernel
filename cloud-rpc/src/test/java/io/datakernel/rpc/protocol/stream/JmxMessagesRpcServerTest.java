package io.datakernel.rpc.protocol.stream;

import io.datakernel.eventloop.Eventloop;
import io.datakernel.promise.Promise;
import io.datakernel.rpc.client.RpcClient;
import io.datakernel.rpc.server.RpcServer;
import io.datakernel.test.rules.EventloopRule;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;

import java.io.IOException;
import java.net.InetSocketAddress;

import static io.datakernel.promise.TestUtils.await;
import static io.datakernel.rpc.client.sender.RpcStrategies.server;
import static io.datakernel.rpc.server.RpcServer.DEFAULT_MAX_MESSAGE_SIZE;
import static io.datakernel.test.TestUtils.getFreePort;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class JmxMessagesRpcServerTest {
	private final int LISTEN_PORT = getFreePort();

	@ClassRule
	public static final EventloopRule eventloopRule = new EventloopRule();

	RpcServer server;

	@Before
	public void setup() throws IOException {
		server = RpcServer.create(Eventloop.getCurrentEventloop())
				.withMessageTypes(String.class)
				.withStreamProtocol(DEFAULT_MAX_MESSAGE_SIZE, DEFAULT_MAX_MESSAGE_SIZE, true)
				.withHandler(String.class, String.class, request ->
						Promise.of("Hello, " + request + "!"))
				.withListenPort(LISTEN_PORT)
				.withAcceptOnce();
		server.listen();
	}

	@Test
	public void testWithoutProtocolError() throws IOException {
		RpcClient client = RpcClient.create(Eventloop.getCurrentEventloop())
				.withMessageTypes(String.class)
				.withStreamProtocol(DEFAULT_MAX_MESSAGE_SIZE, DEFAULT_MAX_MESSAGE_SIZE, true)
				.withStrategy(server(new InetSocketAddress("localhost", LISTEN_PORT)));
		await(client.start().whenResult($ -> {
			client.sendRequest("msg", 1000)
					.whenComplete(() -> {
						assertEquals(0, server.getFailedRequests().getTotalCount());
						client.stop();
					});
		}));
	}

	@Test
	public void testWithProtocolError() throws IOException {
		RpcClient client = RpcClient.create(Eventloop.getCurrentEventloop())
				.withMessageTypes(String.class)
				.withStrategy(server(new InetSocketAddress("localhost", LISTEN_PORT)));
		await(client.start().whenResult($ -> {
			client.sendRequest("msg", 10000)
					.whenComplete(() -> {
						assertTrue(server.getLastProtocolError().getTotal() > 0);
						client.stop();
					});
		}));
	}

	@Test
	public void testWithProtocolError2() throws IOException {
		RpcClient client = RpcClient.create(Eventloop.getCurrentEventloop())
				.withMessageTypes(String.class)
				.withStrategy(server(new InetSocketAddress("localhost", LISTEN_PORT)));
		await(client.start().whenResult($ -> {
			client.sendRequest("Message larger than LZ4 header", 1000)
					.whenComplete(() -> {
						assertTrue(server.getLastProtocolError().getTotal() > 0);
						client.stop();
					});
		}));
	}
}
