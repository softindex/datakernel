package io.datakernel.eventloop;

public interface AsyncUdpSocket {
	interface EventHandler {
		void onRegistered();

		void onSent();

		void onRead(UdpPacket packet);

		void onClosedWithError(Exception e);
	}

	void setEventHandler(AsyncUdpSocket.EventHandler eventHandler);

	void read();

	void send(UdpPacket packet);

	void close();
}
