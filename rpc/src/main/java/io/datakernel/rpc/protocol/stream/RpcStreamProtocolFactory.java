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

package io.datakernel.rpc.protocol.stream;

import io.datakernel.rpc.protocol.*;

import java.nio.channels.SocketChannel;

public final class RpcStreamProtocolFactory implements RpcProtocolFactory {
	private final RpcStreamProtocolSettings settings;

	public RpcStreamProtocolFactory(RpcStreamProtocolSettings settings) {
		this.settings = settings;
	}

	@Override
	public RpcProtocol create(final RpcConnection connection, SocketChannel socketChannel, RpcMessageSerializer serializer, boolean isServer) {
		return new RpcStreamProtocol(connection.getEventloop(), socketChannel, serializer, settings) {
			@Override
			protected void onReceiveMessage(RpcMessage message) {
				connection.onReceiveMessage(message);
			}

			@Override
			protected void onWired() {
				connection.ready();
			}

			@Override
			protected void onClosed() {
				connection.onClosed();
			}
		};
	}
}
