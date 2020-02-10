/*
 * Copyright (C) 2015-2018 SoftIndex LLC.
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

package io.datakernel.net;

import io.datakernel.async.process.Cancellable;
import io.datakernel.bytebuf.ByteBuf;
import io.datakernel.promise.Promise;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 * Common interface for connection-oriented transport protocols.
 * <p>
 * This interface describes asynchronous read and write operations for data transmission through the network.
 * <p>
 * Implementations of this interface should follow rules described below:
 * <ul>
 * <li>Each request to the socket after it was closed should complete exceptionally. <i>This is due to an ability of
 * the socket to be closed before any read/write operation is called. User should be informed about it after he makes first
 * call to {@link #read()} or {@link #write(ByteBuf)}<i/></li>
 * </ul>
 */
public interface AsyncTcpSocket extends Cancellable {
	/**
	 * Operation to read some data from network. Returns a promise of a bytebuf that represents some data received
	 * from network.
	 * <p>
	 * It is allowed to call read before previous read was completed.
	 * However, each consecutive call will cancel all of the previous calls (they will not be completed).
	 *
	 * @return promise of ByteBuf that represents data received from network
	 */
	@NotNull
	Promise<ByteBuf> read();

	/**
	 * Operation to write some data to network. Returns a promise of void that represents successful write.
	 * <p>
	 * Many write operations may be called. However, when some write is successfull, all of the promises received from write calls before it will be completed at once.
	 *
	 * @param buf data to be sent to network
	 * @return promise that represents successfull write operation
	 */
	@NotNull
	Promise<Void> write(@Nullable ByteBuf buf);

}
