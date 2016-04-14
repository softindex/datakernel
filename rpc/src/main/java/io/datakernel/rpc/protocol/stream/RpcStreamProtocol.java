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

import io.datakernel.bytebuf.ByteBuf;
import io.datakernel.eventloop.Eventloop;
import io.datakernel.eventloop.SocketConnection;
import io.datakernel.rpc.protocol.RpcMessage;
import io.datakernel.rpc.protocol.RpcProtocol;
import io.datakernel.serializer.BufferSerializer;
import io.datakernel.stream.*;
import io.datakernel.stream.net.TcpStreamSocketConnection;
import io.datakernel.stream.processor.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.channels.SocketChannel;

abstract class RpcStreamProtocol implements RpcProtocol {
	private static final Logger logger = LoggerFactory.getLogger(RpcStreamProtocol.class);

	private class Receiver extends AbstractStreamConsumer<RpcMessage> implements StreamDataReceiver<RpcMessage> {

		public Receiver(Eventloop eventloop) {
			super(eventloop);
		}

		@Override
		protected void onStarted() {

		}

		@Override
		public StreamDataReceiver<RpcMessage> getDataReceiver() {
			return this;
		}

		@Override
		public void onData(RpcMessage message) {
			onReceiveMessage(message);
		}

		@Override
		protected void onEndOfStream() {
			sender.close();
			connection.close();
		}

		@Override
		protected void onError(Exception e) {
			logger.error("RpcStreamProtocol was closed with error", e);
			sender.closeWithError(e);
			connection.close();
		}

		public void closeWithError(Exception e) {
			super.closeWithError(e);
		}

	}

	private class Sender extends AbstractStreamProducer<RpcMessage> {
		private boolean channelOpen;

		public Sender(Eventloop eventloop) {
			super(eventloop);
			channelOpen = true;
		}

		@Override
		protected void onStarted() {

		}

		@Override
		protected void onDataReceiverChanged() {

		}

		@Override
		public void onSuspended() {
		}

		@Override
		public void onResumed() {
		}

		@Override
		protected void doCleanup() {
			channelOpen = false;
		}

		public boolean isOverloaded() {
			return getProducerStatus() != StreamStatus.READY;
		}

		public void sendMessage(RpcMessage item) {
			if (channelOpen) {
				super.send(item);
			} else {
				// ignore message
			}
		}

		public void close() {
			sendEndOfStream();
		}

		public void closeWithError(Exception e) {
			super.closeWithError(e);
		}

		@Override
		protected void onError(Exception e) {
			// we do not log error here, because it [was logged] / [will be logged] in Receiver
			receiver.closeWithError(e);
		}
	}

	private final Sender sender;
	private final Receiver receiver;
	private final StreamLZ4Compressor compressor;
	private final StreamLZ4Decompressor decompressor;
	private final StreamSerializer<RpcMessage> serializer;
	private final StreamDeserializer<RpcMessage> deserializer;
	private final boolean compression;
	private final TcpStreamSocketConnection connection;

	protected RpcStreamProtocol(Eventloop eventloop, SocketChannel socketChannel,
	                            BufferSerializer<RpcMessage> messageSerializer,
	                            int defaultPacketSize, int maxPacketSize, boolean compression) {
		sender = new Sender(eventloop);
		receiver = new Receiver(eventloop);

		serializer = new StreamBinarySerializer<>(eventloop, messageSerializer, defaultPacketSize,
				maxPacketSize, 0, true);
		deserializer = new StreamBinaryDeserializer<>(eventloop, messageSerializer, maxPacketSize);
		this.compression = compression;
		if (compression) {
			compressor = StreamLZ4Compressor.fastCompressor(eventloop);
			decompressor = new StreamLZ4Decompressor(eventloop);
		} else {
			compressor = null;
			decompressor = null;
		}

		connection = new TcpStreamSocketConnection(eventloop, socketChannel) {
			@Override
			protected void wire(StreamProducer<ByteBuf> socketReader, StreamConsumer<ByteBuf> socketWriter) {
				if (RpcStreamProtocol.this.compression) {
					socketReader.streamTo(decompressor.getInput());
					decompressor.getOutput().streamTo(deserializer.getInput());

					RpcStreamProtocol.this.serializer.getOutput().streamTo(compressor.getInput());
					compressor.getOutput().streamTo(socketWriter);
				} else {
					socketReader.streamTo(deserializer.getInput());
					RpcStreamProtocol.this.serializer.getOutput().streamTo(socketWriter);
				}
				deserializer.getOutput().streamTo(receiver);
				sender.streamTo(RpcStreamProtocol.this.serializer.getInput());

				onWired();
			}

			@Override
			public void onClosed() {
				RpcStreamProtocol.this.onClosed();
			}
		};
	}

	@Override
	public void sendMessage(RpcMessage message) {
		sender.sendMessage(message);
	}

	@Override
	public boolean isOverloaded() {
		return sender.isOverloaded();
	}

	@Override
	public SocketConnection getSocketConnection() {
		return connection;
	}

	protected abstract void onWired();

	protected abstract void onReceiveMessage(RpcMessage message);

	protected abstract void onClosed();

	@Override
	public void close() {
		sender.close();
		if (compression) {
			decompressor.getInput().onProducerEndOfStream();
		} else {
			deserializer.getInput().onProducerEndOfStream();
		}
		connection.close();
	}
}
