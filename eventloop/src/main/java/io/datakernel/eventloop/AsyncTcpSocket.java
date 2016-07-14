/*
 * Copyright (C) 2015 SoftIndex LLC.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.datakernel.eventloop;

import io.datakernel.bytebuf.ByteBuf;

import java.net.InetSocketAddress;

/**
 * Common interface for connection-oriented transport protocols.
 * <p/>
 * <p>
 * Contains operations for reading and writing {@link ByteBuf}.
 * All read and write operations must be performed asynchronously
 * </p>
 * <p/>
 * <p>
 * Implementations are supposed to use Decorator and Observer patterns which allows
 * to easily cascade protocol as in OSI model.
 * For example SSL works on top of TCP, therefore
 * {@link AsyncSslSocket} writes encrypted data to  {@link AsyncTcpSocketImpl}, and at the same time
 * {@link AsyncSslSocket} is event handler
 * of {@link AsyncTcpSocketImpl}
 * </p>
 */
public interface AsyncTcpSocket {
	/**
	 * Handles events of socket
	 */
	interface EventHandler {
		void onRegistered();

		/**
		 * Is called when new input data was received by socket
		 *
		 * @param buf
		 */
		void onRead(ByteBuf buf);

		/**
		 * Is called when other side closed output, namely other side won't send any more data
		 */
		void onShutdownInput();

		/**
		 * Is called when written data was flushed to network
		 */
		void onWrite();

		void onClosedWithError(Exception e);
	}

	void setEventHandler(EventHandler eventHandler);

	/**
	 * Change socket configs to be interested in reading data from network.
	 */
	void read();

	/**
	 * Asynchronously writes data to network.
	 * <p/>
	 * When written data was flushed, EventHandler.onWrite() method will be called
	 *
	 * @param buf bytes of data
	 */
	void write(ByteBuf buf);

	/**
	 * Informs socket that no more writes will be
	 */
	void shutdownOutput();

	void flushAndClose();

	void close();

	InetSocketAddress getRemoteSocketAddress();
}
