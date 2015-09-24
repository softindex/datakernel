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

package io.datakernel.stream.net;

import io.datakernel.bytebuf.ByteBuf;
import io.datakernel.eventloop.NioEventloop;
import io.datakernel.stream.*;
import io.datakernel.stream.processor.StreamDeserializer;
import io.datakernel.stream.processor.StreamSerializer;

import java.nio.channels.SocketChannel;
import java.util.HashMap;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;

/**
 * It is wrapper for  Binary protocol which deserializes received stream to type of input object,
 * processes it with MessageProtocol, getting object for output type, serializes it to ByteBuffer and streams it back.
 *
 * @param <I> type of input object, for MessageProtocol
 * @param <O> type of output object, for MessageProtocol
 */
public class StreamMessagingConnection<I, O> extends TcpStreamSocketConnection implements Messaging<O>, StreamDataReceiver<I> {
	private MessagingStarter<O> starter;
	protected final HashMap<Class<? extends I>, MessagingHandler<? extends I, O>> handlers = new HashMap<>();

	private StreamConsumerSwitcher<ByteBuf> socketReaderSwitcher;
	private StreamProducerSwitcher<ByteBuf> socketWriterSwitcher;

	private final StreamDeserializer<I> streamDeserializer;
	private final StreamSerializer<O> streamSerializer;

	private final MessageConsumer messageConsumer = new MessageConsumer();
	private final MessageProducer messageProducer = new MessageProducer();

	private StreamDataReceiver<O> output;

	/**
	 * Creates a new instance of BinaryProtocolMessaging
	 *
	 * @param eventloop          eventloop in with this connection will be handled
	 * @param socketChannel      socketChannel for this connection
	 * @param streamDeserializer for deserializing input ByteBuffer to object
	 * @param streamSerializer   for serializing obtained output object to ByteBuffer
	 */
	public StreamMessagingConnection(NioEventloop eventloop, SocketChannel socketChannel,
	                                 StreamDeserializer<I> streamDeserializer, StreamSerializer<O> streamSerializer) {
		super(eventloop, socketChannel);
		this.streamDeserializer = streamDeserializer;
		this.streamSerializer = streamSerializer;
		this.socketReaderSwitcher = new StreamConsumerSwitcher<>(eventloop, this.streamDeserializer);
		this.socketWriterSwitcher = new StreamProducerSwitcher<>(eventloop, this.streamSerializer);
		this.streamDeserializer.streamTo(this.messageConsumer);
		this.messageProducer.streamTo(this.streamSerializer);
	}

	public <T extends I> StreamMessagingConnection<I, O> addHandler(Class<T> type, MessagingHandler<T, O> handler) {
		handlers.put(type, handler);
		return this;
	}

	public StreamMessagingConnection<I, O> addStarter(MessagingStarter<O> starter) {
		this.starter = starter;
		return this;
	}

	/**
	 * Organizes connections between streams for deserializing and serializing
	 *
	 * @param socketReader producer which outputs binary data
	 * @param socketWriter consumer which internalConsumers binary data
	 */
	@Override
	protected void wire(StreamProducer<ByteBuf> socketReader, StreamConsumer<ByteBuf> socketWriter) {
		socketReader.streamTo(socketReaderSwitcher);
		socketWriterSwitcher.streamTo(socketWriter);

		output = messageProducer.getDownstreamDataReceiver();
		onStart();
	}

	protected void onStart() {
		if (starter != null) {
			starter.onStart(this);
		}
	}

	private class MessageConsumer extends AbstractStreamConsumer<I> {
		public MessageConsumer() {
			super(StreamMessagingConnection.this.eventloop);
		}

		@Override
		public StreamDataReceiver<I> getDataReceiver() {
			return StreamMessagingConnection.this;
		}

		@Override
		protected void onStarted() {

		}

		@Override
		protected void onEndOfStream() {

		}

		@Override
		protected void onError(Exception e) {

		}
	}

	private class MessageProducer extends AbstractStreamProducer<O> {
		public MessageProducer() {
			super(StreamMessagingConnection.this.eventloop);
		}

		@Override
		protected void onStarted() {

		}

		@Override
		protected void onSuspended() {
		}

		@Override
		protected void onResumed() {
		}

		@Override
		protected void onError(Exception e) {

		}
	}

	@SuppressWarnings("unchecked")
	@Override
	public void onData(I item) {
		Class<?> type = item.getClass();
		MessagingHandler<I, O> handler = (MessagingHandler<I, O>) handlers.get(type);
		checkNotNull(handler, "No handler for message type: %s", type);
		handler.onMessage(item, this);
	}

	@Override
	public void sendMessage(O outputItem) {
		output.onData(outputItem);
	}

	@Override
	public StreamConsumer<ByteBuf> binarySocketWriter() {
		StreamForwarder<ByteBuf> forwarder = new StreamForwarder<>(eventloop);
		streamSerializer.flush();
		socketWriterSwitcher.switchProducerTo(forwarder);
		return forwarder;
	}

	@Override
	public StreamProducer<ByteBuf> binarySocketReader() {
		StreamForwarder<ByteBuf> forwarder = new StreamForwarder<>(eventloop);
		socketReaderSwitcher.switchConsumerTo(forwarder);
		streamDeserializer.drainBuffersTo(forwarder);
		return forwarder;
	}

	@Override
	public void shutdown() {
		eventloop.post(new Runnable() {
			@Override
			public void run() {
				messageConsumer.closeUpstream();
				messageProducer.sendEndOfStream();
			}
		});
	}

	@Override
	public void shutdownReader() {
		checkState(socketReaderSwitcher.getCurrentConsumer() == streamDeserializer, "SocketReader is rewired to another stream");
		eventloop.post(new Runnable() {
			@Override
			public void run() {
				messageConsumer.closeUpstream();
			}
		});
	}

	@Override
	public void shutdownWriter() {
		checkState(socketWriterSwitcher.getCurrentProducer() == streamSerializer, "SocketWriter is rewired to another stream");
		eventloop.post(new Runnable() {
			@Override
			public void run() {
				messageProducer.sendEndOfStream();
			}
		});
	}

}
