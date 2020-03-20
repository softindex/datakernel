/*
 * Copyright (C) 2015-2020 SoftIndex LLC.
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

package io.datakernel.crdt.remote;

import io.datakernel.common.exception.StacklessException;
import io.datakernel.crdt.CrdtClient;
import io.datakernel.crdt.CrdtData.CrdtDataSerializer;
import io.datakernel.csp.net.MessagingWithBinaryStreaming;
import io.datakernel.datastream.StreamConsumer;
import io.datakernel.datastream.csp.ChannelDeserializer;
import io.datakernel.datastream.csp.ChannelSerializer;
import io.datakernel.eventloop.Eventloop;
import io.datakernel.net.AbstractServer;
import io.datakernel.net.AsyncTcpSocket;
import io.datakernel.promise.Promise;
import io.datakernel.serializer.BinarySerializer;

import java.net.InetAddress;

import static io.datakernel.crdt.CrdtMessaging.*;
import static io.datakernel.csp.binary.ByteBufSerializer.ofJsonCodec;

public final class CrdtRemoteServer<K extends Comparable<K>, S> extends AbstractServer<CrdtRemoteServer<K, S>> {
	private final CrdtClient<K, S> client;
	private final CrdtDataSerializer<K, S> serializer;
	private final BinarySerializer<K> keySerializer;

	private CrdtRemoteServer(Eventloop eventloop, CrdtClient<K, S> client, CrdtDataSerializer<K, S> serializer) {
		super(eventloop);
		this.client = client;
		this.serializer = serializer;

		keySerializer = serializer.getKeySerializer();
	}

	public static <K extends Comparable<K>, S> CrdtRemoteServer<K, S> create(Eventloop eventloop, CrdtClient<K, S> client, CrdtDataSerializer<K, S> serializer) {
		return new CrdtRemoteServer<>(eventloop, client, serializer);
	}

	public static <K extends Comparable<K>, S> CrdtRemoteServer<K, S> create(Eventloop eventloop, CrdtClient<K, S> client, BinarySerializer<K> keySerializer, BinarySerializer<S> stateSerializer) {
		return new CrdtRemoteServer<>(eventloop, client, new CrdtDataSerializer<>(keySerializer, stateSerializer));
	}

	@Override
	protected void serve(AsyncTcpSocket socket, InetAddress remoteAddress) {
		MessagingWithBinaryStreaming<CrdtRequest, CrdtResponse> messaging =
				MessagingWithBinaryStreaming.create(socket, ofJsonCodec(REQUEST_CODEC, RESPONSE_CODEC));
		messaging.receive()
				.then(msg -> {
					if (msg == null) {
						return Promise.ofException(new StacklessException(CrdtRemoteServer.class, "Unexpected end of stream"));
					}
					if (msg == CrdtRequests.PING) {
						return messaging.send(CrdtResponses.PONG)
								.whenResult($ -> messaging.close());
					}
					if (msg == CrdtRequests.UPLOAD) {
						return messaging.receiveBinaryStream()
								.transformWith(ChannelDeserializer.create(serializer))
								.streamTo(StreamConsumer.ofPromise(client.upload()))
								.then($ -> messaging.send(CrdtResponses.UPLOAD_FINISHED))
								.then($ -> messaging.sendEndOfStream())
								.whenResult($ -> messaging.close());

					}
					if (msg == CrdtRequests.REMOVE) {
						return messaging.receiveBinaryStream()
								.transformWith(ChannelDeserializer.create(keySerializer))
								.streamTo(StreamConsumer.ofPromise(client.remove()))
								.then($ -> messaging.send(CrdtResponses.REMOVE_FINISHED))
								.then($ -> messaging.sendEndOfStream())
								.whenResult($ -> messaging.close());
					}
					if (msg instanceof Download) {
						return client.download(((Download) msg).getToken())
								.whenResult($ -> messaging.send(CrdtResponses.DOWNLOAD_STARTED))
								.then(supplier -> supplier
										.transformWith(ChannelSerializer.create(serializer))
										.streamTo(messaging.sendBinaryStream()));
					}
					return Promise.ofException(new StacklessException(CrdtRemoteServer.class, "Message type was added, but no handling code for it"));
				})
				.whenComplete(($, e) -> {
					if (e == null) {
						return;
					}
					logger.warn("got an error while handling message (" + e + ") : " + this);
					String prefix = e.getClass() != StacklessException.class ? e.getClass().getSimpleName() + ": " : "";
					messaging.send(new ServerError(prefix + e.getMessage()))
							.then($1 -> messaging.sendEndOfStream())
							.whenResult($1 -> messaging.close());
				});
	}
}
