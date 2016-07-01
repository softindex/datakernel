package io.datakernel.eventloop;

import io.datakernel.net.SocketSettings;

import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLEngine;
import java.net.SocketAddress;
import java.util.concurrent.ExecutorService;

import static io.datakernel.eventloop.AsyncTcpSocket.EventHandler;
import static io.datakernel.util.Preconditions.check;
import static io.datakernel.util.Preconditions.checkNotNull;

public abstract class AbstractConnector<S extends AbstractConnector<S, E>, E extends EventHandler> {
	protected final Eventloop eventloop;
	protected final SocketSettings settings;

	// SSL
	private SSLContext sslContext;
	private ExecutorService executor;

	public AbstractConnector(Eventloop eventloop, SocketSettings settings) {
		this.eventloop = checkNotNull(eventloop);
		this.settings = checkNotNull(settings);
	}

	@SuppressWarnings("unchecked")
	protected S self() {
		return (S) this;
	}

	public S enableSsl(SSLContext sslContext, ExecutorService executor) {
		this.sslContext = checkNotNull(sslContext);
		this.executor = checkNotNull(executor);
		return self();
	}

	public void connect(final SocketAddress address, int timeout, final boolean secure) {
		eventloop.connect(address, settings, timeout, new ConnectCallback() {
			@Override
			public EventHandler onConnect(AsyncTcpSocketImpl asyncTcpSocket) {
				settings.applyReadWriteTimeoutsTo(asyncTcpSocket);

				E eventHandler = createEventHandler(asyncTcpSocket);
				if (secure) {
					check(sslContext != null, "Can't establish secure connection");
					AsyncSslSocket sslSocket = createAsyncSslSocket(asyncTcpSocket);
					sslSocket.setEventHandler(eventHandler);
//					onSuccessfulConnect(eventHandler);
					return sslSocket;
				} else {
					return eventHandler;
				}
			}

			@Override
			public void onException(Exception e) {
//				onConnectFailed(e);
			}

			@Override
			public String toString() {
				return address.toString();
			}
		});
	}

	private AsyncSslSocket createAsyncSslSocket(AsyncTcpSocketImpl asyncTcpSocket) {
		SSLEngine engine = sslContext.createSSLEngine();
		engine.setUseClientMode(true);
		return new AsyncSslSocket(eventloop, asyncTcpSocket, engine, executor);
	}

	protected abstract E createEventHandler(AsyncTcpSocket socket);
}
