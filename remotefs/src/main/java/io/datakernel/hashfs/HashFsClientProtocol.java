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

package io.datakernel.hashfs;

import io.datakernel.async.ResultCallback;
import io.datakernel.eventloop.ConnectCallback;
import io.datakernel.eventloop.Eventloop;
import io.datakernel.eventloop.SocketConnection;
import io.datakernel.hashfs.HashFsCommands.Alive;
import io.datakernel.hashfs.HashFsResponses.ListServers;
import io.datakernel.net.SocketSettings;
import io.datakernel.protocol.ClientProtocol;
import io.datakernel.protocol.FsCommands.FsCommand;
import io.datakernel.protocol.FsResponses.Err;
import io.datakernel.protocol.FsResponses.FsResponse;
import io.datakernel.protocol.FsResponses.ListFiles;
import io.datakernel.stream.net.Messaging;
import io.datakernel.stream.net.MessagingHandler;
import io.datakernel.stream.net.MessagingStarter;
import io.datakernel.stream.net.StreamMessagingConnection;
import io.datakernel.stream.processor.StreamGsonDeserializer;
import io.datakernel.stream.processor.StreamGsonSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetSocketAddress;
import java.nio.channels.SocketChannel;
import java.util.List;
import java.util.Set;

import static io.datakernel.hashfs.HashFsCommands.Offer;
import static io.datakernel.hashfs.HashFsCommands.commandGSON;
import static io.datakernel.hashfs.HashFsResponses.responseGSON;

final class HashFsClientProtocol extends ClientProtocol {
	public static class Builder extends ClientProtocol.Builder<Builder> {
		private Builder(Eventloop eventloop) {
			super(eventloop);
		}

		@Override
		public HashFsClientProtocol build() {
			return new HashFsClientProtocol(eventloop, minChunkSize, maxChunkSize, connectTimeout,
					socketSettings, serializerBufferSize, serializerMaxMessageSize,
					serializerFlushDelayMillis, deserializerBufferSize);
		}
	}

	private static final Logger logger = LoggerFactory.getLogger(ClientProtocol.class);

	private HashFsClientProtocol(Eventloop eventloop, int minChunkSize, int maxChunkSize,
	                             int connectTimeout, SocketSettings socketSettings,
	                             int serializerBufferSize, int serializerMaxMessageSize,
	                             int serializerFlushDelayMillis, int deserializerBufferSize) {
		super(eventloop, minChunkSize, maxChunkSize,
				connectTimeout, socketSettings,
				serializerBufferSize, serializerMaxMessageSize,
				serializerFlushDelayMillis, deserializerBufferSize);
	}

	public static HashFsClientProtocol newInstance(Eventloop eventloop) {
		return new Builder(eventloop).build();
	}

	public static Builder build(Eventloop eventloop) {
		return new Builder(eventloop);
	}

	// api
	public void alive(InetSocketAddress address, ResultCallback<Set<ServerInfo>> callback) {
		connect(address, aliveConnectCallback(callback));
	}

	public void offer(InetSocketAddress address, List<String> forUpload,
	                  List<String> forDeletion, ResultCallback<List<String>> callback) {
		connect(address, offerConnectCallback(forUpload, forDeletion, callback));
	}

	// connect callbacks
	private ConnectCallback aliveConnectCallback(final ResultCallback<Set<ServerInfo>> callback) {
		return new ForwardingConnectCallback(callback) {
			@Override
			public void onConnect(SocketChannel socketChannel) {
				SocketConnection connection = createConnection(socketChannel)
						.addStarter(new MessagingStarter<FsCommand>() {
							@Override
							public void onStart(Messaging<FsCommand> messaging) {
								logger.trace("Send command to show alive servers");
								messaging.sendMessage(new Alive());
							}
						})
						.addHandler(ListServers.class, new MessagingHandler<ListServers, FsCommand>() {
							@Override
							public void onMessage(ListServers item, Messaging<FsCommand> messaging) {
								logger.trace("Received {} alive servers", item.servers.size());
								messaging.shutdown();
								callback.onResult(item.servers);
							}
						})
						.addHandler(Err.class, new MessagingHandler<Err, FsCommand>() {
							@Override
							public void onMessage(Err item, Messaging<FsCommand> messaging) {
								logger.trace("Can't figure out alive servers: {}", item.msg);
								messaging.shutdown();
								Exception e = new Exception(item.msg);
								callback.onException(e);
							}
						});
				connection.register();
			}
		};
	}

	private ConnectCallback offerConnectCallback(final List<String> forUpload, final List<String> forDeletion,
	                                             final ResultCallback<List<String>> callback) {
		return new ForwardingConnectCallback(callback) {
			@Override
			public void onConnect(SocketChannel socketChannel) {
				SocketConnection connection = createConnection(socketChannel)
						.addStarter(new MessagingStarter<FsCommand>() {
							@Override
							public void onStart(Messaging<FsCommand> messaging) {
								logger.trace("Sending offer to download: {} files, to delete: {} files", forUpload.size(), forDeletion.size());
								messaging.sendMessage(new Offer(forDeletion, forUpload));
							}
						})
						.addHandler(ListFiles.class, new MessagingHandler<ListFiles, FsCommand>() {
							@Override
							public void onMessage(ListFiles item, Messaging<FsCommand> messaging) {
								logger.trace("Received response for file offer");
								messaging.shutdown();
								callback.onResult(item.files);
							}
						})
						.addHandler(Err.class, new MessagingHandler<Err, FsCommand>() {
							@Override
							public void onMessage(Err item, Messaging<FsCommand> messaging) {
								logger.trace("Can't receive response for file offer");
								messaging.shutdown();
								Exception e = new Exception(item.msg);
								callback.onException(e);
							}
						});
				connection.register();
			}
		};
	}

	@Override
	protected StreamMessagingConnection<FsResponse, FsCommand> createConnection(SocketChannel socketChannel) {
		return new StreamMessagingConnection<>(eventloop, socketChannel,
				new StreamGsonDeserializer<>(eventloop, responseGSON, FsResponse.class, deserializerBufferSize),
				new StreamGsonSerializer<>(eventloop, commandGSON, FsCommand.class, serializerBufferSize,
						serializerMaxMessageSize, serializerFlushDelayMillis));
	}
}