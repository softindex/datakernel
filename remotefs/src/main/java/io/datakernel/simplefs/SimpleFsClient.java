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

package io.datakernel.simplefs;

import io.datakernel.FsClient;
import io.datakernel.StreamTransformerWithCounter;
import io.datakernel.async.CompletionCallback;
import io.datakernel.async.ResultCallback;
import io.datakernel.bytebuf.ByteBuf;
import io.datakernel.eventloop.Eventloop;
import io.datakernel.net.SocketSettings;
import io.datakernel.protocol.ClientProtocol;
import io.datakernel.stream.StreamProducer;

import java.net.InetSocketAddress;
import java.util.List;

import static io.datakernel.util.Preconditions.checkNotNull;

public final class SimpleFsClient implements FsClient {
	public static final class Builder {
		private final InetSocketAddress address;
		private final ClientProtocol.Builder protocolBuilder;

		private Builder(Eventloop eventloop, InetSocketAddress address) {
			this.protocolBuilder = ClientProtocol.build(eventloop);
			this.address = address;
		}

		public Builder setMinChunkSize(int minChunkSize) {
			protocolBuilder.setMinChunkSize(minChunkSize);
			return this;
		}

		public Builder setConnectTimeout(int connectTimeout) {
			protocolBuilder.setConnectTimeout(connectTimeout);
			return this;
		}

		public Builder setSocketSettings(SocketSettings socketSettings) {
			protocolBuilder.setSocketSettings(socketSettings);
			return this;
		}

		public Builder setSerializerBufferSize(int serializerBufferSize) {
			protocolBuilder.setSerializerBufferSize(serializerBufferSize);
			return this;
		}

		public Builder setSerializerMaxMessageSize(int serializerMaxMessageSize) {
			protocolBuilder.setSerializerMaxMessageSize(serializerMaxMessageSize);
			return this;
		}

		public Builder setSerializerFlushDelayMillis(int serializerFlushDelayMillis) {
			protocolBuilder.setSerializerFlushDelayMillis(serializerFlushDelayMillis);
			return this;
		}

		public Builder setMaxChunkSize(int maxChunkSize) {
			protocolBuilder.setMaxChunkSize(maxChunkSize);
			return this;
		}

		public Builder setDeserializerBufferSize(int deserializerBufferSize) {
			protocolBuilder.setDeserializerBufferSize(deserializerBufferSize);
			return this;
		}

		public SimpleFsClient build() {
			return new SimpleFsClient(address, protocolBuilder.build());
		}
	}

	private final InetSocketAddress serverAddress;
	private final ClientProtocol protocol;

	// creators
	private SimpleFsClient(InetSocketAddress serverAddress, ClientProtocol protocol) {
		this.serverAddress = checkNotNull(serverAddress);
		this.protocol = checkNotNull(protocol);
	}

	public static SimpleFsClient newInstance(InetSocketAddress address, ClientProtocol protocol) {
		return new SimpleFsClient(address, protocol);
	}

	public static SimpleFsClient newInstance(Eventloop eventloop, InetSocketAddress address) {
		return new SimpleFsClient.Builder(eventloop, address).build();
	}

	public static Builder build(Eventloop eventloop, InetSocketAddress address) {
		return new Builder(eventloop, address);
	}

	// api
	@Override
	public void upload(String fileName, StreamProducer<ByteBuf> producer, CompletionCallback callback) {
		protocol.upload(serverAddress, fileName, producer, callback);
	}

	@Override
	public void download(String fileName, long startPosition, ResultCallback<StreamTransformerWithCounter> callback) {
		protocol.download(serverAddress, fileName, startPosition, callback);
	}

	@Override
	public void list(final ResultCallback<List<String>> callback) {
		protocol.list(serverAddress, callback);
	}

	@Override
	public void delete(String fileName, CompletionCallback callback) {
		protocol.delete(serverAddress, fileName, callback);
	}
}