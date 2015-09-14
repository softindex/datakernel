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
import io.datakernel.eventloop.NioEventloop;
import io.datakernel.eventloop.SocketConnection;
import io.datakernel.jmx.CompositeDataBuilder;
import io.datakernel.jmx.MBeanFormat;
import io.datakernel.rpc.protocol.RpcMessage;
import io.datakernel.rpc.protocol.RpcMessageSerializer;
import io.datakernel.rpc.protocol.RpcProtocol;
import io.datakernel.stream.*;
import io.datakernel.stream.net.TcpStreamSocketConnection;
import io.datakernel.stream.processor.*;

import javax.management.openmbean.CompositeData;
import javax.management.openmbean.OpenDataException;
import javax.management.openmbean.SimpleType;
import java.nio.channels.SocketChannel;

import static com.google.common.base.Preconditions.checkNotNull;

abstract class RpcStreamProtocol implements RpcProtocol {

	private class Receiver extends AbstractStreamConsumer<RpcMessage> implements StreamDataReceiver<RpcMessage> {

		public Receiver(Eventloop eventloop) {
			super(eventloop);
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
		public void onProducerEndOfStream() {
			sender.sendEndOfStream();
		}

		@Override
		public void onProducerError(Exception e) {
			sender.onConsumerError(e);
		}
	}

	private class Sender extends AbstractStreamProducer<RpcMessage> {

		public Sender(Eventloop eventloop) {
			super(eventloop);
		}

		@Override
		public void onSuspended() {
		}

		@Override
		public void onResumed() {
		}

		@Override
		public void onClosed() {
			receiver.closeUpstream();
		}

		@Override
		public void onClosedWithError(Exception e) {
			receiver.closeUpstreamWithError(e);
		}

		public boolean isOverloaded() {
			return status != READY;
		}

		public void sendMessage(RpcMessage message) throws Exception {
			downstreamDataReceiver.onData(message);
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
	// JMX
	private boolean monitoring;
	private long timeMonitoring;

	protected RpcStreamProtocol(NioEventloop eventloop, SocketChannel socketChannel, RpcMessageSerializer messageSerializer, RpcStreamProtocolSettings settings) {
		sender = new Sender(eventloop);
		receiver = new Receiver(eventloop);

		serializer = new StreamBinarySerializer<>(eventloop, checkNotNull(messageSerializer).getSerializer(), settings.getDefaultPacketSize(),
				settings.getMaxPacketSize(), 0, true);
		deserializer = new StreamBinaryDeserializer<>(eventloop, checkNotNull(messageSerializer).getSerializer(), settings.getMaxPacketSize());
		compression = settings.isCompression();
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
				if (compression) {
					socketReader.streamTo(decompressor);
					decompressor.streamTo(deserializer);

					serializer.streamTo(compressor);
					compressor.streamTo(socketWriter);
				} else {
					socketReader.streamTo(deserializer);
					serializer.streamTo(socketWriter);
				}
				deserializer.streamTo(receiver);
				sender.streamTo(serializer);

				onWired();
			}

			@Override
			public void onClosed() {
				RpcStreamProtocol.this.onClosed();
			}
		};
	}

	@Override
	public void sendMessage(RpcMessage message) throws Exception {
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
		sender.sendEndOfStream();
		receiver.closeUpstream();
	}

	// JMX
	public void startMonitoring() {
		monitoring = true;
		timeMonitoring = System.currentTimeMillis();
	}

	public void stopMonitoring() {
		monitoring = false;
		timeMonitoring = 0;
	}

	public boolean isMonitoring() {
		return monitoring;
	}

	public void reset() {
		if (isMonitoring())
			timeMonitoring = System.currentTimeMillis();
	}

	private String getMonitoringTime() {
		if (timeMonitoring == 0)
			return null;
		return MBeanFormat.formatDuration(System.currentTimeMillis() - timeMonitoring);
	}

	@Override
	public CompositeData getConnectionDetails() throws OpenDataException {
		return CompositeDataBuilder.builder(RpcStreamProtocol.class.getSimpleName(), "Rpc stream connection details")
				.add("Overloaded", SimpleType.BOOLEAN, isOverloaded())
				.add("Compression", SimpleType.BOOLEAN, compression)
				.add("Monitoring", SimpleType.BOOLEAN, isMonitoring())
				.add("MonitoringTime", SimpleType.STRING, getMonitoringTime())
				.add("ChannelInfo", SimpleType.STRING, connection.getChannelInfo())
				.build();
	}
}
