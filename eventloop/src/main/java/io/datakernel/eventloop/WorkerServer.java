package io.datakernel.eventloop;

import io.datakernel.net.SocketSettings;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.nio.channels.SocketChannel;

public interface WorkerServer {
	Eventloop getEventloop();

	void doAccept(SocketChannel socketChannel, InetAddress remoteAddress, InetSocketAddress localAddress,
	              SocketSettings socketSettings, final boolean ssl);
}
