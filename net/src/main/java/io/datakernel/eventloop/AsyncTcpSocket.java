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

import io.datakernel.annotation.Nullable;
import io.datakernel.async.Cancellable;
import io.datakernel.async.Promise;
import io.datakernel.bytebuf.ByteBuf;
import io.datakernel.net.SocketSettings;

import java.nio.channels.SocketChannel;

import static io.datakernel.eventloop.Eventloop.getCurrentEventloop;

/**
 * Common interface for connection-oriented transport protocols.
 * <p>
 * This interface describes asynchronous read and write operations for data transmission through network.
 * <p>
 * <p>
 * Implementations of this interface should follow rules described below:
 * <ul>
 * <li>Each request to socket after it has been closed should be completed exceptionally. <i>This is due to an ability of
 * socket to be closed before any read/write operation is called. User should be informed about it after he makes first
 * call to {@link #read()} or {@link #write(ByteBuf)}<i/></li>
 * </ul>
 */
public interface AsyncTcpSocket extends Cancellable {
	/**
	 * Operation to read some data from network. Returns a promise of a bytebuf that represents some data recieved
	 * from network.
	 * <p>
	 * It is allowed to call {@link #read()} before previous {@link #read()} was completed.
	 * However, each consecutive call will cancel all of the previous calls (they will not be completed).
	 *
	 * @return promise of ByteBuf that represents data recieved from network
	 */
	Promise<ByteBuf> read();

	/**
	 * Operation to write some data to network. Returns a promise of void that represents succesfull write.
	 * <p>
	 * Many {@link #write(ByteBuf)} operations may be called. However, when write is succesful,
	 * all of the previous promises that wait on write will be completed.
	 *
	 * @param buf data to be sent to network
	 * @return promise that represents succesful write operation
	 */
	Promise<Void> write(@Nullable ByteBuf buf);

	static AsyncTcpSocket ofSocketChannel(SocketChannel socketChannel) {
		return AsyncTcpSocketImpl.wrapChannel(getCurrentEventloop(), socketChannel, null);
	}

	// region builders
	static AsyncTcpSocket ofSocketChannel(SocketChannel socketChannel, SocketSettings socketSettings) {
		return AsyncTcpSocketImpl.wrapChannel(getCurrentEventloop(), socketChannel, socketSettings);
	}

}
