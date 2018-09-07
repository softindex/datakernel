package io.global.globalfs.server;

import io.datakernel.eventloop.AbstractServer;
import io.datakernel.eventloop.AsyncTcpSocket;
import io.datakernel.eventloop.Eventloop;

public class GlobalFsServer extends AbstractServer<GlobalFsServer> {

	public GlobalFsServer(Eventloop eventloop) {
		super(eventloop);
	}

	@Override
	protected AsyncTcpSocket.EventHandler createSocketHandler(AsyncTcpSocket asyncTcpSocket) {

		throw new UnsupportedOperationException("todo");
	}
}
