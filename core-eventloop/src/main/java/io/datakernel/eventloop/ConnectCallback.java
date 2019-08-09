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

import org.jetbrains.annotations.NotNull;

import java.nio.channels.SocketChannel;

/**
 * Client-side version of {@link AcceptCallback}.
 * <p>
 * It is used as a callback for completing connections to some server
 * when using {@link Eventloop} at the lowest level - calling {@link Eventloop#connect}.
 */
public interface ConnectCallback {
	/**
	 * Called from the eventloop thread when the connection is established.
	 *
	 * @param socketChannel established connection.
	 */
//	@Async.Execute
	void onConnect(@NotNull SocketChannel socketChannel);

	/**
	 * Called from the eventloop thread when the connection was failed.
	 *
	 * @param e some raised exception, usually a low-level IOException from NIO.
	 */
//	@Async.Execute
	void onException(@NotNull Throwable e);
}
