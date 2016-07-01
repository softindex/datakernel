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

public interface AsyncTcpSocket {
	interface EventHandler {
		void onRegistered();

		void onRead(ByteBuf buf);

		void onReadEndOfStream();

		void onWrite();

		void onClosedWithError(Exception e);
	}

	void setEventHandler(EventHandler eventHandler);

	void read();

	void write(ByteBuf buf);

	void writeEndOfStream();

	void close();

	InetSocketAddress getRemoteSocketAddress();
}
