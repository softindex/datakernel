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
import io.datakernel.eventloop.Eventloop;
import io.datakernel.eventloop.SocketConnection;
import io.datakernel.protocol.FsCommands;
import io.datakernel.protocol.FsResponses.Err;
import io.datakernel.protocol.FsResponses.FsResponse;
import io.datakernel.protocol.ServerProtocol;
import io.datakernel.stream.net.Messaging;
import io.datakernel.stream.net.MessagingHandler;
import io.datakernel.stream.net.StreamMessagingConnection;
import io.datakernel.stream.processor.StreamGsonDeserializer;
import io.datakernel.stream.processor.StreamGsonSerializer;

import java.nio.channels.SocketChannel;
import java.util.List;
import java.util.Set;

import static io.datakernel.hashfs.HashFsCommands.commandGSON;
import static io.datakernel.hashfs.HashFsResponses.responseGSON;

final class HashFsServerProtocol extends ServerProtocol<HashFsServer> {
	public static final class Builder extends ServerProtocol.Builder<Builder, HashFsServer> {
		private Builder(Eventloop eventloop) {
			super(eventloop);
		}

		@Override
		public HashFsServerProtocol build() {
			return new HashFsServerProtocol(eventloop, serializerBufferSize,
					serializerMaxMessageSize, serializerFlushDelayMillis, deserializerBufferSize);
		}
	}

	private HashFsServerProtocol(Eventloop eventloop, int serializerBufferSize,
	                             int serializerMaxMessageSize, int serializerFlushDelayMillis,
	                             int deserializerBufferSize) {
		super(eventloop, serializerBufferSize,
				serializerMaxMessageSize, serializerFlushDelayMillis, deserializerBufferSize);
	}

	public static HashFsServerProtocol newInstance(Eventloop eventloop) {
		return new Builder(eventloop).build();
	}

	public static Builder build(Eventloop eventloop) {
		return new Builder(eventloop);
	}

	protected MessagingHandler<HashFsCommands.Alive, FsResponse> defineAliveHandler() {
		return new MessagingHandler<HashFsCommands.Alive, FsResponse>() {
			@Override
			public void onMessage(HashFsCommands.Alive item, final Messaging<FsResponse> messaging) {
				server.showAlive(new ResultCallback<Set<ServerInfo>>() {
					@Override
					public void onResult(Set<ServerInfo> result) {
						messaging.sendMessage(new HashFsResponses.ListServers(result));
						messaging.shutdown();
					}

					@Override
					public void onException(Exception e) {
						messaging.sendMessage(new Err(e.getMessage()));
						messaging.shutdown();
					}
				});
			}
		};
	}

	protected MessagingHandler<HashFsCommands.Offer, FsResponse> defineOfferHandler() {
		return new MessagingHandler<HashFsCommands.Offer, FsResponse>() {
			@Override
			public void onMessage(HashFsCommands.Offer item, final Messaging<FsResponse> messaging) {
				server.checkOffer(item.forUpload, item.forDeletion, new ResultCallback<List<String>>() {
					@Override
					public void onResult(List<String> result) {
						messaging.sendMessage(new HashFsResponses.ListFiles(result));
						messaging.shutdown();
					}

					@Override
					public void onException(Exception e) {
						messaging.sendMessage(new Err(e.getMessage()));
						messaging.shutdown();
					}
				});
			}
		};
	}

	@Override
	protected SocketConnection createConnection(SocketChannel socketChannel) {
		return new StreamMessagingConnection<>(eventloop, socketChannel,
				new StreamGsonDeserializer<>(eventloop, commandGSON, FsCommands.FsCommand.class, deserializerBufferSize),
				new StreamGsonSerializer<>(eventloop, responseGSON, FsResponse.class, serializerBufferSize,
						serializerMaxMessageSize, serializerFlushDelayMillis))
				.addHandler(FsCommands.Upload.class, defineUploadHandler())
				.addHandler(FsCommands.Download.class, defineDownloadHandler())
				.addHandler(FsCommands.Delete.class, defineDeleteHandler())
				.addHandler(FsCommands.ListFiles.class, defineListFilesHandler())
				.addHandler(HashFsCommands.Alive.class, defineAliveHandler())
				.addHandler(HashFsCommands.Offer.class, defineOfferHandler());
	}
}