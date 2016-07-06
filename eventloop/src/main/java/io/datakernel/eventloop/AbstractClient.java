package io.datakernel.eventloop;

import io.datakernel.async.ExceptionCallback;
import io.datakernel.net.SocketSettings;

import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLEngine;
import java.net.SocketAddress;
import java.util.concurrent.ExecutorService;

import static io.datakernel.eventloop.AsyncTcpSocket.EventHandler;
import static io.datakernel.util.Preconditions.check;
import static io.datakernel.util.Preconditions.checkNotNull;

public abstract class AbstractClient<S extends AbstractClient<S>> {
	protected final Eventloop eventloop;
	protected final SocketSettings settings;

	// SSL
	private SSLContext sslContext;
	private ExecutorService executor;

	public AbstractClient(Eventloop eventloop, SocketSettings settings) {
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

	public void connect(final SocketAddress address, int timeout, final boolean secure, final SpecialConnectCallback callback) {
		eventloop.connect(address, settings, timeout, new ConnectCallback() {
			@Override
			public EventHandler onConnect(AsyncTcpSocketImpl asyncTcpSocket) {
				settings.applyReadWriteTimeoutsTo(asyncTcpSocket);
				if (secure) {
					check(sslContext != null, "Can't establish secure connection");
					AsyncSslSocket sslSocket = createAsyncSslSocket(asyncTcpSocket);
					asyncTcpSocket.setEventHandler(sslSocket);
					EventHandler handler = callback.onConnect(sslSocket);
					sslSocket.setEventHandler(handler);
					return sslSocket;
				} else {
					EventHandler handler = callback.onConnect(asyncTcpSocket);
					asyncTcpSocket.setEventHandler(handler);
					return handler;
				}
			}

			@Override
			public void onException(Exception e) {
				callback.onException(e);
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

	public interface SpecialConnectCallback extends ExceptionCallback {
		EventHandler onConnect(AsyncTcpSocket socket);
	}
}
