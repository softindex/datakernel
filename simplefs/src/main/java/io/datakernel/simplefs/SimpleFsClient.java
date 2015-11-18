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

import io.datakernel.async.CompletionCallback;
import io.datakernel.async.ForwardingResultCallback;
import io.datakernel.async.ResultCallback;
import io.datakernel.bytebuf.ByteBuf;
import io.datakernel.eventloop.NioEventloop;
import io.datakernel.net.SocketSettings;
import io.datakernel.stream.StreamConsumer;
import io.datakernel.stream.StreamProducer;

import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import static io.datakernel.async.AsyncCallbacks.ignoreCompletionCallback;

public final class SimpleFsClient {
	public static final class Builder {
		private final InetSocketAddress address;
		private final GsonClientProtocol.Builder protocolBuilder;

		private Builder(NioEventloop eventloop, InetSocketAddress address) {
			this.address = address;
			protocolBuilder = GsonClientProtocol.buildInstance(eventloop);
		}

		public Builder setMinChunkSize(int minChunkSize) {
			protocolBuilder.setMinChunkSize(minChunkSize);
			return this;
		}

		public Builder setSerializerBufferSize(int serializerBufferSize) {
			protocolBuilder.setSerializerBufferSize(serializerBufferSize);
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

		public Builder setSerializerFlushDelayMillis(int serializerFlushDelayMillis) {
			protocolBuilder.setSerializerFlushDelayMillis(serializerFlushDelayMillis);
			return this;
		}

		public Builder setConnectTimeout(int connectTimeout) {
			protocolBuilder.setConnectTimeout(connectTimeout);
			return this;
		}

		public Builder setSerializerMaxMessageSize(int serializerMaxMessageSize) {
			protocolBuilder.setSerializerMaxMessageSize(serializerMaxMessageSize);
			return this;
		}

		public Builder setSocketSettings(SocketSettings socketSettings) {
			protocolBuilder.setSocketSettings(socketSettings);
			return this;
		}

		public SimpleFsClient build() {
			GsonClientProtocol protocol = protocolBuilder.build();
			return new SimpleFsClient(address, protocol);
		}
	}

	private final InetSocketAddress serverAddress;
	private final GsonClientProtocol protocol;

	private SimpleFsClient(InetSocketAddress serverAddress, GsonClientProtocol protocol) {
		this.serverAddress = serverAddress;
		this.protocol = protocol;
	}

	public static SimpleFsClient createInstance(NioEventloop eventloop, InetSocketAddress address) {
		return buildInstance(eventloop, address).build();
	}

	public static Builder buildInstance(NioEventloop eventloop, InetSocketAddress address) {
		return new Builder(eventloop, address);
	}

	public void upload(String fileName, StreamProducer<ByteBuf> producer, CompletionCallback callback) {
		protocol.upload(serverAddress, fileName, producer, callback);
	}

	public void download(String fileName, StreamConsumer<ByteBuf> consumer) {
		protocol.download(serverAddress, fileName, consumer, ignoreCompletionCallback());
	}

	public void list(final ResultCallback<List<String>> callback) {
		protocol.list(serverAddress, new ForwardingResultCallback<Set<String>>(callback) {
			@Override
			public void onResult(Set<String> result) {
				callback.onResult(new ArrayList<>(result));
			}
		});
	}

	public void delete(String fileName, CompletionCallback callback) {
		protocol.delete(serverAddress, fileName, callback);
	}
}