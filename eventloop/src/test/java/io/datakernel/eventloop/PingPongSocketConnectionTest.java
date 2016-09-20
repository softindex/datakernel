package io.datakernel.eventloop;

import io.datakernel.bytebuf.ByteBuf;
import io.datakernel.eventloop.SimpleServer.SocketHandlerProvider;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.channels.SocketChannel;

import static io.datakernel.bytebuf.ByteBufPool.*;
import static io.datakernel.bytebuf.ByteBufStrings.decodeAscii;
import static io.datakernel.bytebuf.ByteBufStrings.wrapAscii;
import static io.datakernel.helper.TestUtils.doesntHaveFatals;
import static org.junit.Assert.*;

public class PingPongSocketConnectionTest {
	private final Logger logger = LoggerFactory.getLogger(this.getClass());

	private final InetSocketAddress ADDRESS = new InetSocketAddress("localhost", 9022);
	private final int ITERATIONS = 3;
	private final String RESPONSE_MSG = "PONG";
	private final String REQUEST_MSG = "PING";

	@Test
	public void test() throws IOException {
		final Eventloop eventloop = Eventloop.create();

		SocketHandlerProvider socketHandlerProvider = new SocketHandlerProvider() {
			@Override
			public AsyncTcpSocket.EventHandler createSocketHandler(final AsyncTcpSocket asyncTcpSocket) {
				return new ServerConnection(asyncTcpSocket);
			}
		};

		final SimpleServer ppServer = SimpleServer.create(eventloop, socketHandlerProvider).withListenAddress(ADDRESS);

		ppServer.listen();

		eventloop.connect(ADDRESS, new ConnectCallback() {
			@Override
			public void onConnect(SocketChannel socketChannel) {
				AsyncTcpSocketImpl asyncTcpSocket = AsyncTcpSocketImpl.wrapChannel(eventloop, socketChannel);
				asyncTcpSocket.setEventHandler(new ClientConnection(asyncTcpSocket, ppServer));
				asyncTcpSocket.register();
			}

			@Override
			public void onException(Exception e) {
				fail("Exception: " + e);
			}
		});

		eventloop.run();
		assertEquals(getPoolItemsString(), getCreatedItems(), getPoolItems());
		assertThat(eventloop, doesntHaveFatals());
	}

	private class ServerConnection implements AsyncTcpSocket.EventHandler {
		private final AsyncTcpSocket asyncTcpSocket;
		int counter;

		ServerConnection(AsyncTcpSocket asyncTcpSocket) {
			this.asyncTcpSocket = asyncTcpSocket;
			counter = 0;
		}

		@Override
		public void onRegistered() {
			asyncTcpSocket.read();
		}

		@Override
		public void onReadEndOfStream() {
			asyncTcpSocket.close();
			assertEquals(ITERATIONS, counter);
		}

		@Override
		public void onRead(ByteBuf buf) {
			assertEquals(REQUEST_MSG, decodeAscii(buf));
			buf.recycle();
			counter++;
			asyncTcpSocket.write(wrapAscii(RESPONSE_MSG));
		}

		@Override
		public void onWrite() {
			asyncTcpSocket.read();
		}

		@Override
		public void onClosedWithError(Exception e) {
			e.printStackTrace();
		}
	}

	private class ClientConnection implements AsyncTcpSocket.EventHandler {
		private final AsyncTcpSocketImpl clientTcpSocket;
		private final AbstractServer server;
		int counter;

		ClientConnection(AsyncTcpSocketImpl clientTcpSocket, AbstractServer server) {
			this.clientTcpSocket = clientTcpSocket;
			this.server = server;
			counter = 0;
		}

		@Override
		public void onRegistered() {
			clientTcpSocket.write(wrapAscii(REQUEST_MSG));
		}

		@Override
		public void onRead(ByteBuf buf) {
			assertEquals(RESPONSE_MSG, decodeAscii(buf));
			if (++counter == ITERATIONS) {
				clientTcpSocket.close();
				server.close();
			} else {
				clientTcpSocket.write(wrapAscii(REQUEST_MSG));
			}
			buf.recycle();
		}

		@Override
		public void onReadEndOfStream() {
		}

		@Override
		public void onWrite() {
			clientTcpSocket.read();
		}

		@Override
		public void onClosedWithError(Exception e) {
			e.printStackTrace();
		}
	}
}
