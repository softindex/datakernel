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

package io.datakernel.hashfs.stub.upload_big;

import io.datakernel.async.ResultCallback;
import io.datakernel.bytebuf.ByteBuf;
import io.datakernel.eventloop.Eventloop;
import io.datakernel.eventloop.NioEventloop;
import io.datakernel.hashfs.ServerInfo;
import io.datakernel.stream.StreamConsumer;
import io.datakernel.stream.StreamConsumers;
import io.datakernel.stream.StreamForwarder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetSocketAddress;
import java.util.List;

public class Client {

	private static final Logger logger = LoggerFactory.getLogger(Client.class);

	private final Eventloop eventloop;
	private final ClientProtocol protocol;

	private Client(Eventloop eventloop, ClientProtocol protocol) {
		this.eventloop = eventloop;
		this.protocol = protocol;
	}

	public static Client createHashClient(NioEventloop eventloop) {
		ClientProtocol clientTransport = new ClientProtocol(eventloop);
		return new Client(eventloop, clientTransport);
	}

	private void getAliveServers(InetSocketAddress address, final ResultCallback<List<ServerInfo>> callback) {

		protocol.getAliveServers(address, new ResultCallback<List<ServerInfo>>() {
			@Override
			public void onResult(List<ServerInfo> result) {
				callback.onResult(result);
			}

			@Override
			public void onException(Exception exception) {
			}
		});
	}

	private void upload(final String filename, InetSocketAddress address, final ResultCallback<StreamConsumer<ByteBuf>> callback) {

		protocol.upload(address, filename, new ResultCallback<StreamConsumer<ByteBuf>>() {
			@Override
			public void onResult(StreamConsumer<ByteBuf> streamConsumer) {
				callback.onResult(streamConsumer);
			}

			@Override
			public void onException(Exception exception) {
			}
		});
	}

	private void upload(final InetSocketAddress address, final String filename, final ResultCallback<StreamConsumer<ByteBuf>> callback) {
		getAliveServers(address, new ResultCallback<List<ServerInfo>>() {
			@Override
			public void onException(Exception exception) {
			}

			@Override
			public void onResult(List<ServerInfo> aliveServers) {
				upload(filename, address, callback);
			}
		});
	}

	public StreamConsumer<ByteBuf> upload(InetSocketAddress address, String destinationFilePath) {
		final StreamForwarder<ByteBuf> forwarder = new StreamForwarder<>(eventloop);
		upload(address, destinationFilePath, new ResultCallback<StreamConsumer<ByteBuf>>() {
			@Override
			public void onResult(StreamConsumer<ByteBuf> actualConsumer) {
				forwarder.streamTo(actualConsumer);
			}

			@Override
			public void onException(Exception exception) {
				logger.error("fail", exception);
				forwarder.streamTo(StreamConsumers.<ByteBuf>closingWithError(eventloop, exception));
			}
		});
		return forwarder;
	}

}
