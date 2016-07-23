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
 *
 * <p>
 * Contains operations for reading and writing {@link ByteBuf}.
 * All read and write operations must be performed asynchronously
 * </p>
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
		 * Is called when new input data was received by socket.
		 * EventHandler must be ready to handle any read data event if it has not been requested.
		 *
		 * @param buf
		 */
		void onRead(ByteBuf buf);

		/**
		 * Is called when other side closed output, namely other side won't send any more data
		 */
		void onReadEndOfStream();

		/**
		 * Is called when all buffered data is flushed to network.
		 * This callback is called once per {@link AsyncTcpSocket#write(ByteBuf)} invocation, unless the socket is closed.
		 */
		void onWrite();

		void onClosedWithError(Exception e);
	}

	void setEventHandler(EventHandler eventHandler);

	/**
	 * Must be called to inform the async socket that additional read data is needed.
	 * Unless called, the socket does not guarantee (but still can) to return any read data in its onRead callback.
	 * Each read invocation may result in one (or more) onRead callbacks.
	 * This operation is idempotent and may be called several times prior actual onRead callbacks.
	 */
	void read();

	/**
	 * Asynchronously writes data to network.
	 * <p/>
	 * When written data is flushed, {@link EventHandler#onWrite()} method will be called
	 *
	 * @param buf bytes of data
	 */
	void write(ByteBuf buf);

	/**
	 * Informs socket that no more data will be written.
	 * Any pending write data is expected to be delivered with corresponding {@link EventHandler#onWrite()} confirmation.
	 * This operation is idempotent.
	 * No subsequent {@link EventHandler#onWrite()} operations on the socket must be called afterwards.
	 */
	void writeEndOfStream();

	/**
	 * Closes socket, regardless of any remaining buffered data. This operation is idempotent.
	 * No subsequent operations on the socket must be called afterwards.
	 * <p>
	 * After calling this method no more event handler methods will be invoked
	 * </p>
	 */
	void close();

	InetSocketAddress getRemoteSocketAddress();
}
