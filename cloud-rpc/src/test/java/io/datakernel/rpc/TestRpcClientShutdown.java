package io.datakernel.rpc;

import io.datakernel.eventloop.Eventloop;
import io.datakernel.promise.Promise;
import io.datakernel.promise.Promises;
import io.datakernel.rpc.client.RpcClient;
import io.datakernel.rpc.client.RpcClientConnection;
import io.datakernel.rpc.server.RpcServer;
import io.datakernel.test.rules.ByteBufRule;
import io.datakernel.test.rules.EventloopRule;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.List;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;

import static io.datakernel.promise.TestUtils.awaitException;
import static io.datakernel.rpc.client.RpcClientConnection.CONNECTION_CLOSED;
import static io.datakernel.rpc.client.sender.RpcStrategies.server;
import static io.datakernel.test.TestUtils.getFreePort;
import static java.util.Arrays.asList;
import static org.junit.Assert.assertSame;

public final class TestRpcClientShutdown {
	@ClassRule
	public static final ByteBufRule byteBufRule = new ByteBufRule();
	public static final int PORT = getFreePort();

	@Rule
	public final EventloopRule eventloopRule = new EventloopRule();

	@Test
	public void testServerOnClientShutdown() throws IOException {
		Eventloop eventloop = Eventloop.getCurrentEventloop();
		Executor executor = Executors.newSingleThreadExecutor();
		List<Class<?>> messageTypes = asList(Request.class, Response.class);

		RpcServer rpcServer = RpcServer.create(eventloop)
				.withMessageTypes(messageTypes)
				.withHandler(Request.class, Response.class,
						request -> Promise.ofBlockingCallable(executor, () -> {
							Thread.sleep(100);
							return new Response();
						}))
				.withListenPort(PORT);

		RpcClient rpcClient = RpcClient.create(eventloop)
				.withMessageTypes(messageTypes)
				.withStrategy(server(new InetSocketAddress(PORT)));

		rpcServer.listen();

		Throwable exception = awaitException(rpcClient.start()
				.then(() -> Promises.all(
						rpcClient.sendRequest(new Request())
								.whenComplete(() -> {
									for (RpcClientConnection conn : rpcClient.getRequestStatsPerConnection().values()) {
										conn.onSenderError(new Error());
									}
								}),
						rpcClient.sendRequest(new Request()),
						rpcClient.sendRequest(new Request()),
						rpcClient.sendRequest(new Request()),
						rpcClient.sendRequest(new Request())
				))
				.whenComplete(rpcClient::stop)
				.whenComplete(rpcServer::close)
		);

		assertSame(CONNECTION_CLOSED, exception);
	}

	public static final class Request {
	}

	public static final class Response {
	}
}
